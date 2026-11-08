"""
Running the stages for each file.

A file that needs extracting or chunking goes through work(), which
reads the file, converts it, chunks it and writes the docling-v2.json,
.md and chunks.jsonl files beside it. It hands the chunks to finish(),
which embeds them and writes the embeddings and the final status.json.

A reader never sees a status that vouches for files it doesn't describe:
before a stage overwrites files that the status says are complete, a
status is written with that stage's entries removed (outcome ``failed``,
error "superseded: <stage> running again"), then the files, then the
final status. A run stopped in between leaves a status that says the
stage still needs running.

run_file() does both for one file in this process, for a Lambda or a
test. run_files() does many, with work() in a pool of worker processes
if it's given one, so that Docling's models are loaded once a worker
rather than once a file. dispatch() does what can be done without
Docling: it settles the files that aren't documents to extract, and
says which need a converter.

An error in Docling, docling-core or the embedding model while doing
one file is recorded in its status, and the run carries on. An error in
the store itself writes nothing, so the next run tries the file again.
"""

import datetime
import hashlib
import tempfile
import time
import traceback
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field, replace
from pathlib import Path

from lgsf.embeddings import extract
from lgsf.embeddings.chunking import chunk_document, document_title, inputs_hash
from lgsf.embeddings.embedders import EmbedderBase
from lgsf.embeddings.extract import (
    EXTRACTED,
    LEFT_OUT_FORMATS,
    ConverterSettings,
    ExtractStatus,
)
from lgsf.embeddings.files import File
from lgsf.embeddings.formats import PDF_FORMATS, DocumentFormat, detect_format
from lgsf.embeddings.records import (
    Chunk,
    chunks_jsonl,
    embed_chunks,
    embeddings_jsonl,
    read_chunks,
    read_embeddings,
)
from lgsf.embeddings.sidecars import (
    OUTCOMES,
    ChunkEntry,
    EmbedEntry,
    ExtractEntry,
    FilePlan,
    FileStatus,
    Outcome,
    Stage,
    Suffix,
    embeddings_suffix,
    needs_embed,
    parse_status,
    plan_file,
    read_status,
    result_key,
    write_status,
)


@dataclass
class Options:
    stages: Sequence[Stage] = tuple(Stage)
    #: Leave PDFs with more pages than this for a later run.
    max_pages: int | None = None
    #: Run every stage again, even for files already done.
    force: bool = False
    #: Extract again files that failed or timed out before.
    retry_failed: bool = False
    #: How Docling runs, in this process or the workers.
    converter: ConverterSettings = field(default_factory=ConverterSettings)


class InputMissing(Exception):
    """
    A stage's input file (the Docling document, the chunks) isn't in the
    store or can't be read, so the stage that makes it has to run again.
    """

    def __init__(self, stage: Stage, suffix: str, cause: BaseException):
        super().__init__(f"{suffix} missing or unreadable ({_error(cause)})")
        self.stage = stage
        self.suffix = suffix


@dataclass
class WorkResult:
    """What work() hands to finish() for one file."""

    format: DocumentFormat | None = None
    #: Set if the file was extracted this time.
    extract: ExtractEntry | None = None
    #: What went wrong extracting, or what Docling reported about a
    #: partial conversion.
    error: str | None = None
    #: Set if the document was chunked this time.
    chunk: ChunkEntry | None = None
    chunks: list[Chunk] | None = None
    #: The file isn't in the store. Nothing is recorded, so it's looked
    #: for again next time.
    missing: bool = False
    #: The PDF has more pages than max_pages. Nothing is recorded, so a
    #: later run picks it up.
    too_long: int | None = None
    #: A stage whose entries to clear from the status, because its output
    #: is missing and the stage wasn't run this time.
    reset: Stage | None = None
    #: Why, for the run's report.
    note: str | None = None


@dataclass
class FileOutcome:
    """What happened to one file, for the caller to report."""

    file: File
    plan: FilePlan
    status: FileStatus | None = None
    result: WorkResult = field(default_factory=WorkResult)
    #: The embedders that embedded the chunks this time.
    embedded_with: list[str] = field(default_factory=list)
    #: Nothing needed doing, and nothing was written.
    skipped: bool = False
    #: The file needs Docling, which dispatch() doesn't have.
    needs_converter: bool = False
    #: An error that stopped the file before its status was written. The
    #: next run tries the file again.
    error: str | None = None
    #: Something recorded in the status worth saying in the report.
    note: str | None = None
    seconds: float = 0.0


def _now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds")


def _error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


# Planning


def current_inputs_hash(status: FileStatus | None) -> str | None:
    """What chunking.inputs_hash() is for a file now, if it's extracted."""
    if status is None or not status.extracted:
        return None
    return inputs_hash(status.format, status.extract.tool_version, status.content_hash)


def _plan(file: File, status: FileStatus | None, options: Options, names) -> FilePlan:
    return plan_file(
        status,
        current_inputs_hash(status),
        options.stages,
        list(names),
        retry_failed=options.retry_failed,
        force=options.force,
    )


def plan(
    store, file: File, options: Options, embedder_names: Sequence[str]
) -> FilePlan:
    """Read one file's status and decide what it needs."""
    status = read_status(store, file.document_key, file.content_hash)
    return _plan(file, status, options, embedder_names)


def check_inputs(status: FileStatus | None, listing) -> FileStatus | None:
    """
    A status with the entries of any stage whose output isn't in
    ``listing`` (the keys in the documents directory) removed, so the
    stage runs again. A status vouching for a file that isn't there
    would otherwise fail every run.
    """
    if status is None:
        return None
    key = f"{status.council}/documents/{status.storage_key}"
    if status.extracted and result_key(key, Suffix.DOCLING) not in listing:
        return replace(status, extract=None, chunk=None, embed={})
    if status.chunked and result_key(key, Suffix.CHUNKS) not in listing:
        return replace(status, chunk=None, embed={})
    return status


def plan_files(
    store,
    files: Sequence[File],
    options: Options,
    embedder_names: Sequence[str],
    threads: int = 16,
) -> list[FilePlan]:
    """
    A plan for each file, from one listing of each council's documents
    directory: only the status files the listing has are read, in
    threads, rather than one request for every file. Each plan carries
    the file's size from the listing.
    """
    sizes = {}
    for council in sorted({file.council for file in files}):
        sizes.update(store.list_sizes(f"{council}/documents/"))

    def read(file):
        key = result_key(file.document_key, Suffix.STATUS)
        if key not in sizes:
            return None
        return parse_status(store.read_bytes(key), file.document_key, file.content_hash)

    with ThreadPoolExecutor(max_workers=threads) as pool:
        statuses = list(pool.map(read, files))

    plans = []
    for file, status in zip(files, statuses, strict=True):
        file_plan = _plan(file, check_inputs(status, sizes), options, embedder_names)
        file_plan.size = sizes.get(file.document_key)
        plans.append(file_plan)
    return plans


# Extracting and chunking: in a worker process, or this one


def _superseded(store, file: File, status: FileStatus | None, stage: Stage):
    """
    Before a stage overwrites files the status vouches for, write the
    status without that stage's entries (and those after it), so that a
    run stopped before the final status leaves nothing vouched for that
    isn't there.
    """
    if status is None or not status.result_suffixes():
        return
    if stage == Stage.EXTRACT:
        changed = replace(status, extract=None, chunk=None, embed={})
    else:
        changed = replace(status, chunk=None, embed={})
    changed.outcome = Outcome.FAILED
    changed.error = f"superseded: {stage} running again"
    write_status(store, file.document_key, changed)


def work(
    file: File,
    status: FileStatus | None,
    do_extract: bool,
    store,
    options: Options,
) -> WorkResult:
    """
    Extract a file if ``do_extract``, then chunk it if the chunk stage is
    run and there's a converted document: the one just made, or the one
    saved before. Errors in the store propagate; see work_safely().
    """
    result = WorkResult(format=status.format if status else None)
    document = data = None
    if do_extract:
        extract.configure(options.converter)
        result, document, data = _extract(file, status, store, options)
        if result.missing or result.too_long:
            return result
        extracted = result.extract.status in EXTRACTED
    else:
        extracted = status is not None and status.extracted

    if Stage.CHUNK in options.stages and extracted:
        docling_version = (
            result.extract.tool_version
            if result.extract
            else status.extract.tool_version
        )
        try:
            result.chunk, result.chunks = _chunk(
                file, status, document, data, result.format, docling_version, store
            )
        except InputMissing as missing:
            if Stage.EXTRACT in options.stages and not do_extract:
                # The Docling document has gone: convert the file again now.
                return work(file, status, True, store, options)
            result.reset = missing.stage
            result.note = f"{missing}: {missing.stage} runs again"
    return result


def work_safely(file, status, do_extract, store, options):
    """
    work(), with an error that isn't Docling's (the store, the file's
    bytes) returned as text rather than raised, so one file's error is
    reported and the run carries on. Nothing is recorded for the file.
    """
    try:
        return work(file, status, do_extract, store, options), None
    except Exception:  # noqa: BLE001
        return None, traceback.format_exc()


def _sha256(path: Path) -> str:
    """The file's SHA-256 as hex, read a megabyte at a time."""
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _inspect(file: File, store, directory: Path):
    """
    Download the file and settle what can be settled without Docling:
    that it's there, that its bytes are the ones its name says, and its
    format. Returns the WorkResult, with an extract entry if the file is
    left out, and the downloaded file's path (None if it's missing).

    The file is never held in memory here: it's streamed to
    ``directory``, hashed in blocks, and its format read from its head.
    A dispatch Lambda sees every file a council stores, agenda packs
    included.
    """
    start = time.monotonic()

    def entry(status):
        return ExtractEntry(
            status=status,
            seconds=round(time.monotonic() - start, 1),
            finished_at=_now(),
        )

    try:
        path = store.download_to(file.document_key, directory)
    except FileNotFoundError:
        return WorkResult(missing=True), None

    digest = _sha256(path)
    result = WorkResult(format=detect_format(path))
    if digest != file.digest:
        result.extract = entry(ExtractStatus.FAILED)
        result.error = (
            f"The file's SHA-256 is {digest}, not the {file.digest} in its name"
        )
        return result, path
    if result.format in LEFT_OUT_FORMATS:
        result.extract = entry(LEFT_OUT_FORMATS[result.format])
    return result, path


def _extract(file: File, status: FileStatus | None, store, options: Options):
    """
    Returns the WorkResult, the DoclingDocument if there is one, and
    the bytes that were converted.
    """
    start = time.monotonic()

    def entry(extract_status, **fields):
        return ExtractEntry(
            status=extract_status,
            seconds=round(time.monotonic() - start, 1),
            finished_at=_now(),
            tool_version=extract.docling_version(),
            device=extract.converter_device(),
            **fields,
        )

    with tempfile.TemporaryDirectory() as directory:
        result, path = _inspect(file, store, Path(directory))
        if result.missing or result.extract is not None:
            return result, None, None
        # Read whole only here, where it's about to be converted.
        raw = path.read_bytes()

    try:
        data = extract.document_bytes(raw, result.format)
        if result.format in PDF_FORMATS and options.max_pages:
            pages = extract.count_pages(data)
            if pages is not None and pages > options.max_pages:
                return WorkResult(too_long=pages), None, None
        conversion = extract.convert(
            extract.document_stream(data, file.storage_key, result.format)
        )
        if conversion.document is not None:
            docling_json = extract.docling_json(conversion.document)
            markdown = conversion.document.export_to_markdown().encode("utf-8")
    except Exception as exc:  # noqa: BLE001
        # Anything Docling raises, recorded for this file alone.
        result.extract = entry(ExtractStatus.FAILED)
        result.error = _error(exc)
        return result, None, None

    if conversion.document is not None:
        # Store errors propagate from here: nothing is recorded, and the
        # next run converts the file again.
        _superseded(store, file, status, Stage.EXTRACT)
        store.write_bytes(result_key(file.document_key, Suffix.DOCLING), docling_json)
        store.write_bytes(result_key(file.document_key, Suffix.TEXT), markdown)

    result.extract = entry(conversion.status, pages=conversion.pages)
    result.error = conversion.error
    return result, conversion.document, data


def _chunk(file: File, status, document, data, document_format, docling_version, store):
    """
    Returns the ChunkEntry, and the chunks if chunking succeeded.

    ``document`` and ``data`` are the DoclingDocument and converted bytes
    from an extraction just done; when the file is only being chunked
    again, they're read from the store. Raises InputMissing if the
    Docling document isn't there or can't be read.
    """
    if document is None:
        docling_key = result_key(file.document_key, Suffix.DOCLING)
        try:
            document = extract.load_docling_json(store.read_bytes(docling_key))
        except Exception as exc:  # noqa: BLE001
            # FileNotFoundError, pydantic's ValidationError, a JSON error
            raise InputMissing(Stage.EXTRACT, Suffix.DOCLING, exc) from exc
    if document_format in PDF_FORMATS and data is None:
        data = extract.document_bytes(
            store.read_bytes(file.document_key), document_format
        )

    try:
        metadata_title = (
            extract.pdf_title(data) if document_format in PDF_FORMATS else None
        )
        chunks = chunk_document(
            file.council,
            file.storage_key,
            document,
            document_title(document, metadata_title),
        )
        lines = chunks_jsonl(chunks)
    except Exception as exc:  # noqa: BLE001
        # Anything docling-core raises, recorded for this file alone.
        return ChunkEntry(inputs_hash=None, finished_at=_now(), error=_error(exc)), None

    _superseded(store, file, status, Stage.CHUNK)
    store.write_bytes(result_key(file.document_key, Suffix.CHUNKS), lines)
    return (
        ChunkEntry(
            inputs_hash=inputs_hash(
                document_format, docling_version, file.content_hash
            ),
            finished_at=_now(),
            chunks=len(chunks),
            embeddable=sum(chunk.embed for chunk in chunks),
        ),
        chunks,
    )


# Embedding and the status: in the parent process


def finish(
    file: File,
    file_plan: FilePlan,
    result: WorkResult,
    store,
    embedders: Sequence[EmbedderBase],
    options: Options,
) -> FileOutcome:
    """Embed the chunks and write the file's status.json."""
    outcome = FileOutcome(file=file, plan=file_plan, result=result)
    if result.missing or result.too_long:
        return outcome

    if file_plan.status is not None:
        status = replace(file_plan.status, embed=dict(file_plan.status.embed))
    else:
        status = FileStatus(
            council=file.council,
            storage_key=file.storage_key,
            content_hash=file.content_hash,
        )
    if result.extract is not None:
        # A new extraction: what was chunked and embedded before is of
        # the old one.
        status.format = result.format
        status.extract = result.extract
        status.outcome = OUTCOMES[result.extract.status]
        status.error = result.error
        status.chunk = None
        status.embed = {}
    if result.chunk is not None:
        status.chunk = result.chunk
    if result.reset == Stage.EXTRACT:
        status.extract = None
        status.chunk = None
        status.embed = {}
        status.outcome = Outcome.FAILED
        status.error = result.note
        outcome.note = result.note

    if Stage.EMBED in options.stages and status.chunked:
        chunks = result.chunks
        for embedder in embedders:
            rechunked = result.chunk is not None
            if not (rechunked or needs_embed(status, embedder.name, options.force)):
                continue
            if chunks is None:
                try:
                    chunks = read_chunks(
                        store.read_bytes(result_key(file.document_key, Suffix.CHUNKS))
                    )
                except (FileNotFoundError, ValueError, TypeError, KeyError) as exc:
                    # The chunks have gone: chunk again next run, rather
                    # than fail on them every run.
                    status.chunk = None
                    status.embed = {}
                    note = f"{Suffix.CHUNKS} missing or unreadable ({_error(exc)}): "
                    note += f"{Stage.CHUNK} runs again"
                    status.error = outcome.note = note
                    break
            if status.embedded(embedder.name):
                _superseded_embed(store, file, status, embedder.name)
            status.embed[embedder.name] = _embed(
                file, status, chunks, embedder, store, options.force
            )
            outcome.embedded_with.append(embedder.name)

    write_status(store, file.document_key, status)
    outcome.status = status
    return outcome


def _superseded_embed(store, file: File, status: FileStatus, name: str):
    """As _superseded(), for one embedder's file."""
    changed = replace(
        status, embed={k: v for k, v in status.embed.items() if k != name}
    )
    changed.outcome = Outcome.FAILED
    changed.error = f"superseded: {Stage.EMBED} running again"
    write_status(store, file.document_key, changed)


def _read_embeddings(store, key: str):
    """
    The embeddings file written before, if there is one. It's read even
    without a status entry: embed_chunks() only reuses a record made from
    the same text by the same model, so a record is either right or not
    used.
    """
    try:
        data = store.read_bytes(key)
    except FileNotFoundError:
        return {}
    try:
        return read_embeddings(data)
    except (ValueError, TypeError):
        return {}


def _embed(file: File, status, chunks, embedder, store, force) -> EmbedEntry:
    key = result_key(file.document_key, embeddings_suffix(embedder.name))
    existing = {} if force else _read_embeddings(store, key)
    try:
        embedded = embed_chunks(chunks, embedder, existing)
    except Exception as exc:  # noqa: BLE001
        # The embedder itself failing, rather than one of its requests.
        return EmbedEntry(
            model=embedder.model,
            chunk_inputs_hash=status.chunk.inputs_hash,
            finished_at=_now(),
            error=_error(exc),
        )
    store.write_bytes(key, embeddings_jsonl(embedded.records))
    error = None
    if embedded.failures:
        wanted = sum(chunk.embed for chunk in chunks)
        error = (
            f"{len(embedded.failures)} of {wanted} chunks not embedded: "
            f"{embedded.failures[0]}"
        )
    # A reused record is the one read from the file.
    sent = [r for r in embedded.records if existing.get(r.chunk_id) is not r]
    return EmbedEntry(
        model=embedder.model,
        chunk_inputs_hash=status.chunk.inputs_hash,
        finished_at=_now(),
        embedded=embedded.embedded,
        reused=embedded.reused,
        tokens=sum(record.token_count for record in sent),
        error=error,
    )


def _finish_safely(file, file_plan, result, store, embedders, options) -> FileOutcome:
    """
    finish(), with an error writing to the store reported for that file
    alone, rather than ending the run.
    """
    try:
        return finish(file, file_plan, result, store, embedders, options)
    except Exception:  # noqa: BLE001
        return FileOutcome(
            file=file, plan=file_plan, result=result, error=traceback.format_exc()
        )


# Whole files


def _do(file, file_plan, store, embedders, options) -> FileOutcome:
    """Every stage a planned file needs, in this process."""
    start = time.monotonic()
    if file_plan.nothing_to_do:
        return FileOutcome(
            file=file, plan=file_plan, status=file_plan.status, skipped=True
        )
    result = WorkResult()
    if file_plan.needs_converter:
        result, error = work_safely(
            file, file_plan.status, file_plan.extract, store, options
        )
        if error is not None:
            return FileOutcome(
                file=file,
                plan=file_plan,
                error=error,
                seconds=round(time.monotonic() - start, 1),
            )
    outcome = _finish_safely(file, file_plan, result, store, embedders, options)
    outcome.seconds = round(time.monotonic() - start, 1)
    return outcome


def run_file(
    store,
    file: File,
    options: Options,
    embedders: Sequence[EmbedderBase] = (),
) -> FileOutcome:
    """
    Every stage one file needs, in this process. Nothing is written if
    it needs none, or if the store fails.
    """
    file_plan = plan(store, file, options, [e.name for e in embedders])
    return _do(file, file_plan, store, embedders, options)


def dispatch(
    store,
    file: File,
    options: Options | None = None,
    embedders: Sequence[EmbedderBase] = (),
) -> FileOutcome:
    """
    What can be done for one file without Docling. A file that needs
    nothing, or only embedding, is finished here. One that has to be
    extracted is downloaded and looked at: if it isn't a document to
    extract (restricted, empty, not a document, or not the bytes its
    name says), its status is written; otherwise nothing is written and
    the outcome says ``needs_converter``, as it does for a file to chunk.
    """
    options = options or Options()
    file_plan = plan(store, file, options, [e.name for e in embedders])
    if file_plan.nothing_to_do or not file_plan.needs_converter:
        return _do(file, file_plan, store, embedders, options)
    if file_plan.chunk:
        return FileOutcome(file=file, plan=file_plan, needs_converter=True)
    try:
        with tempfile.TemporaryDirectory() as directory:
            result, _ = _inspect(file, store, Path(directory))
    except Exception:  # noqa: BLE001
        return FileOutcome(file=file, plan=file_plan, error=traceback.format_exc())
    if result.missing or result.extract is not None:
        return _finish_safely(file, file_plan, result, store, embedders, options)
    return FileOutcome(file=file, plan=file_plan, result=result, needs_converter=True)


def run_files(
    store,
    files: Sequence[File],
    options: Options,
    embedders: Sequence[EmbedderBase] = (),
    executor=None,
    on_outcome: Callable[[FileOutcome], None] | None = None,
    plans: Sequence[FilePlan] | None = None,
) -> list[FileOutcome]:
    """
    Every stage each file needs. Files that need Docling are given to
    ``executor`` if there is one, smallest first so that one slow pack
    doesn't hold up the end, and done here otherwise. While they run,
    the files that only need embedding are done here.

    ``plans`` are the files' plans if the caller has read them already,
    as for a dry run; otherwise plan_files() reads them now.

    If a worker process dies, every file still in the pool is reported
    with the error and BrokenProcessPool is raised at the end, once.
    """
    if plans is None:
        plans = plan_files(store, files, options, [e.name for e in embedders])
    outcomes = []

    def report(outcome):
        outcomes.append(outcome)
        if on_outcome:
            on_outcome(outcome)

    futures = {}
    broken = False
    try:
        to_work = [
            (file, file_plan)
            for file, file_plan in zip(files, plans, strict=True)
            if file_plan.needs_converter and executor is not None
        ]
        for file, file_plan in sorted(to_work, key=lambda pair: pair[1].size or 0):
            future = executor.submit(
                work_safely, file, file_plan.status, file_plan.extract, store, options
            )
            futures[future] = (file, file_plan, time.monotonic())

        for file, file_plan in zip(files, plans, strict=True):
            if file_plan.needs_converter and executor is not None:
                continue
            report(_do(file, file_plan, store, embedders, options))

        for future in as_completed(futures):
            file, file_plan, start = futures[future]
            try:
                result, error = future.result()
            except BrokenProcessPool as exc:
                broken = True
                result, error = None, _error(exc)
            except Exception:  # noqa: BLE001
                # Something work_safely() couldn't return. Nothing is
                # recorded, so the file is tried again next time.
                result, error = None, traceback.format_exc()
            if error is not None:
                outcome = FileOutcome(file=file, plan=file_plan, error=error)
            else:
                outcome = _finish_safely(
                    file, file_plan, result, store, embedders, options
                )
            outcome.seconds = round(time.monotonic() - start, 1)
            report(outcome)
    except BaseException:
        # Stopped, or a worker died. What's been written is kept, and the
        # rest is done by the next run.
        if futures:
            executor.shutdown(wait=False, cancel_futures=True)
        raise
    if broken:
        raise BrokenProcessPool(
            "A worker process stopped, perhaps because it ran out of memory"
        )
    return outcomes
