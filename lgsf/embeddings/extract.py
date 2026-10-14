"""
Converting the PDFs linked from decisions with Docling, and saving the
results for DocumentChunker.

Docling takes about a second a page on a CPU, so `manage.py embeddings
extract` converts each PDF once and saves the DoclingDocument as JSON.
Chunking reads the saved JSON, so the chunking rules can change without
converting again. For each council:

    data/<COUNCIL>/Embeddings/extracted/<storage key>.json
    data/<COUNCIL>/Embeddings/extracted/log.jsonl

A file is named after the key it's stored under in Decisions/documents/,
without its extension: 2026-08-06-1172-s65106.pdf is extracted to
2026-08-06-1172-s65106.json. A file linked under several storage keys is
extracted once, and named after the key of the earliest decision linking
to it.

The log, not the directory, records what has been extracted. It gets a
line each time a file is tried, and the last line for a content_hash is
the one that counts. A JSON file with no line in the log is ignored, and
extracted again by the next run.

Files are converted in a pool of worker processes. Each worker loads
Docling's models once, writes each file's JSON to a temporary file and
renames it when it's complete, then hands its result to the parent
process, which adds the line to the log. Docling is only imported in the
workers, so this module imports without the embeddings dependency group,
and choosing and logging files can be tested without loading Docling's
models.
"""

import json
import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

from lgsf.embeddings.chunkers import ChunkerBase, LinkedDocument

#: Under the council's Embeddings directory. No embedding model can have
#: this name, as embeddings go in a directory named after the model.
EXTRACTED_DIR_NAME = "extracted"

# What the log records for a file. Docling's ConversionStatus has SUCCESS,
# PARTIAL_SUCCESS and FAILURE. A timeout is reported as PARTIAL_SUCCESS
# with a timeout error, and is kept separate here.
OK = "ok"
PARTIAL = "partial"
FAILED = "failed"
TIMEOUT = "timeout"
NOT_PDF = "not_pdf"

#: Statuses with a saved JSON file, for DocumentChunker to read.
EXTRACTED = (OK, PARTIAL)

#: Statuses that are only tried again when asked to.
FAILURES = (FAILED, TIMEOUT)

#: Added to a file's name while a worker writes it.
TEMP_SUFFIX = ".tmp"


def extracted_path(data_path: Path) -> Path:
    """The extracted directory in a council's data directory."""
    return data_path / ChunkerBase.OUTPUT_DIR_NAME / EXTRACTED_DIR_NAME


def json_name(storage_key: str) -> str:
    """The name of the JSON extracted from the file stored under a key."""
    return Path(storage_key).with_suffix(".json").name


class ExtractionLog:
    """log.jsonl in a council's extracted directory."""

    def __init__(self, directory: Path):
        self.directory = directory
        self.path = directory / "log.jsonl"

    def read(self) -> dict[str, dict]:
        """The last line for each content_hash."""
        entries = {}
        if not self.path.exists():
            return entries
        with self.path.open(encoding="utf-8") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    # Cut off by a run that was stopped while writing it.
                    continue
                entries[entry["content_hash"]] = entry
        return entries

    def append(self, entry: dict):
        self.directory.mkdir(parents=True, exist_ok=True)
        with self.path.open("a+b") as f:
            # Start a new line if a stopped run left a line unfinished.
            if f.seek(0, os.SEEK_END) and _last_byte(f) != b"\n":
                f.write(b"\n")
            f.write(json.dumps(entry, ensure_ascii=False).encode("utf-8") + b"\n")

    def extracted_file(self, entry: dict | None) -> Path | None:
        """The JSON file a log entry records, if it has one."""
        if entry and entry.get("status") in EXTRACTED and entry.get("file"):
            return self.directory / entry["file"]
        return None


def _last_byte(f):
    f.seek(-1, os.SEEK_END)
    return f.read(1)


def needs_extracting(
    entry: dict | None, retry_failed: bool = False, force: bool = False
) -> bool:
    """
    Whether to extract a file, given its last line in the log.

    A file that was extracted, partly extracted or found not to be a PDF
    isn't tried again unless `force`. One that failed or timed out is
    only tried again with `retry_failed`, so that a PDF that times out
    doesn't take up its whole timeout on every run.
    """
    if force or entry is None:
        return True
    if entry.get("status") in FAILURES:
        return retry_failed
    return False


def is_pdf(path: Path) -> bool:
    """
    Whether a file is a PDF. Some stored with a .pdf name are ModernGov's
    HTML pages, such as "Reasons restricted".
    """
    with path.open("rb") as f:
        return f.read(5) == b"%PDF-"


def count_pages(path: Path) -> int | None:
    """The number of pages in a PDF, or None if it can't be read."""
    try:
        import pypdfium2

        pdf = pypdfium2.PdfDocument(path)
        try:
            return len(pdf)
        finally:
            pdf.close()
    except Exception:
        return None


@dataclass
class ExtractionTask:
    """One PDF to convert."""

    council: str
    document: LinkedDocument
    log: ExtractionLog
    pages: int | None
    size: int

    @property
    def target(self) -> Path:
        return self.log.directory / json_name(self.document.storage_key)

    def sort_key(self):
        # Fewest pages first, so the longest files run last and a problem
        # with one of them doesn't hold up the rest. A file whose pages
        # can't be counted is probably broken, and fails quickly.
        return (self.pages or 0, self.size)


@dataclass
class CouncilPlan:
    """What a run will do for one council, and what it found to skip."""

    council: str
    tasks: list[ExtractionTask] = field(default_factory=list)
    #: Linked documents with a line in the log that means they're not
    #: tried again.
    done: int = 0
    #: Linked documents that aren't on disk.
    missing: int = 0
    #: Linked documents found not to be PDFs by this run, and logged.
    not_pdf: int = 0


def plan_council(
    council: str,
    documents: dict[str, LinkedDocument],
    log: ExtractionLog,
    docling_version: str,
    retry_failed: bool = False,
    force: bool = False,
) -> CouncilPlan:
    """
    Choose which of a council's linked documents to extract.

    Files that aren't PDFs are logged as not_pdf here, rather than sent
    to a worker. Files that aren't on disk aren't logged, so they're
    looked for again next time.
    """
    plan = CouncilPlan(council)
    entries = log.read()
    for document in documents.values():
        if not needs_extracting(
            entries.get(document.content_hash), retry_failed, force
        ):
            plan.done += 1
            continue
        if not document.path.is_file():
            plan.missing += 1
            continue
        if not is_pdf(document.path):
            log.append(
                log_entry(
                    document,
                    {"status": NOT_PDF, "file": None, "pages": None, "seconds": 0},
                    docling_version,
                )
            )
            plan.not_pdf += 1
            continue
        plan.tasks.append(
            ExtractionTask(
                council=council,
                document=document,
                log=log,
                pages=count_pages(document.path),
                size=document.path.stat().st_size,
            )
        )
    return plan


def remove_temp_files(log: ExtractionLog) -> int:
    """
    Delete files left half written by a run that was stopped. Only one
    run at a time should extract a council's files.
    """
    removed = 0
    if log.directory.is_dir():
        for path in log.directory.glob(f"*{TEMP_SUFFIX}"):
            path.unlink()
            removed += 1
    return removed


def log_entry(document: LinkedDocument, result: dict, docling_version: str) -> dict:
    return {
        "content_hash": document.content_hash,
        "storage_key": document.storage_key,
        "file": result.get("file"),
        "status": result["status"],
        "pages": result.get("pages"),
        "seconds": result.get("seconds"),
        "error": result.get("error"),
        "docling_version": docling_version,
    }


# In the worker processes


#: The worker's DocumentConverter, set up by init_worker().
_converter = None


def init_worker(threads: int, timeout: float):
    """
    Set up a worker process: limit the threads its libraries start, then
    load Docling's models.

    The thread variables have to be set before torch is imported. Without
    them, several workers each start a thread for every core, and run
    more slowly together than one worker does alone.
    """
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS"):
        os.environ[name] = str(threads)
    # The parent shows progress, so progress bars and informational
    # messages from the workers only get in its way.
    os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
    os.environ["TRANSFORMERS_VERBOSITY"] = "error"

    import logging

    import torch
    from docling.datamodel.accelerator_options import AcceleratorOptions
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    # Such as "1 of 248 pdf cells matched neither a row nor a column band".
    for name in ("", "docling", "RapidOCR"):
        logging.getLogger(name).setLevel(logging.ERROR)
    torch.set_num_threads(threads)

    # Docling's default pipeline: its layout model, TableFormer for
    # tables, and OCR of regions with no text layer.
    options = PdfPipelineOptions(
        document_timeout=timeout,
        accelerator_options=AcceleratorOptions(num_threads=threads, device="cpu"),
    )
    global _converter
    _converter = DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=options)}
    )
    _converter.initialize_pipeline(InputFormat.PDF)


def _convert(source: Path):
    """
    Convert one PDF with the worker's converter.

    Returns (status, pages, error, document). The document is None unless
    the status is ok or partial.
    """
    from docling.datamodel.base_models import ConversionStatus

    result = _converter.convert(source, raises_on_error=False)
    pages = result.input.page_count
    error = "; ".join(e.error_message for e in result.errors) or None
    if result.has_timeout_errors():
        return TIMEOUT, pages, error, None
    if result.status == ConversionStatus.SUCCESS:
        return OK, pages, error, result.document
    if result.status == ConversionStatus.PARTIAL_SUCCESS:
        return PARTIAL, pages, error, result.document
    return FAILED, pages, error or f"Docling status: {result.status.value}", None


def _save(document, path: Path):
    from docling_core.types.doc import ImageRefMode

    # The default pipeline doesn't keep images, so there are none to save.
    document.save_as_json(path, image_mode=ImageRefMode.PLACEHOLDER)


def extract_file(source: Path, target: Path) -> dict:
    """
    In a worker: convert one PDF and save the result to `target`.

    The JSON is written to a temporary file and renamed when it's
    complete, so a file with the target's name is never half written.
    Returns the result for the log.
    """
    start = time.monotonic()
    file = None
    try:
        status, pages, error, document = _convert(source)
        if document is not None:
            # The parent only makes the directory when it logs a result,
            # which can be after the first file is saved.
            target.parent.mkdir(parents=True, exist_ok=True)
            temp = target.with_name(target.name + TEMP_SUFFIX)
            _save(document, temp)
            os.replace(temp, target)
            file = target.name
    except Exception as exc:
        status, pages, error = FAILED, None, f"{type(exc).__name__}: {exc}"
    return {
        "status": status,
        "file": file,
        "pages": pages,
        "seconds": round(time.monotonic() - start, 1),
        "error": error,
    }


# In the parent process


def worker_pool(workers: int, threads: int, timeout: float) -> ProcessPoolExecutor:
    """
    Worker processes, each with its own converter.

    They're started with spawn rather than fork, so that each imports
    torch afresh after its thread variables are set.
    """
    return ProcessPoolExecutor(
        max_workers=workers,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=init_worker,
        initargs=(threads, timeout),
    )


def run_tasks(tasks, executor, docling_version: str, on_result=None):
    """
    Convert each task's PDF, in the order given, and log each result as
    it comes back. Calls on_result(task, entry) after logging each one.
    """
    futures = {
        executor.submit(extract_file, task.document.path, task.target): task
        for task in tasks
    }
    try:
        for future in as_completed(futures):
            task = futures[future]
            entry = log_entry(task.document, future.result(), docling_version)
            task.log.append(entry)
            if on_result:
                on_result(task, entry)
    except BaseException:
        # Stopped, or a worker died. What's been logged is kept, and the
        # rest is extracted by the next run.
        executor.shutdown(wait=False, cancel_futures=True)
        raise
