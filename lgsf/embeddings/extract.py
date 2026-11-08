"""
Converting documents with Docling.

Docling takes about a second a page on a CPU, so a run over many files
converts them in a pool of worker processes, each with its own
DocumentConverter. Docling is only imported inside the functions that
use it, so this module imports without the embeddings dependency group.
"""

import importlib.metadata
import io
import multiprocessing
import os
import tempfile
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from lgsf.embeddings.formats import (
    DocumentFormat,
    as_word_document,
    stream_name,
    unwrap_multipart,
)

#: Docling's major version: the ``v2`` in ``.docling-v2.json``. The exact
#: version that converted a file is in its status.
DOCLING_MAJOR_VERSION = 2


class ExtractStatus(StrEnum):
    """What happened when a file was extracted."""

    # Files sent to Docling. Docling's ConversionStatus has SUCCESS,
    # PARTIAL_SUCCESS and FAILURE. A timeout is reported as
    # PARTIAL_SUCCESS with a timeout error, and is kept separate here.
    OK = "ok"
    PARTIAL = "partial"
    FAILED = "failed"
    TIMEOUT = "timeout"

    # Files recognised from their bytes and left out, without loading
    # Docling. See formats.py.
    RESTRICTED = "restricted"
    NOT_DOCUMENT = "not_document"
    EMPTY = "empty"
    UNSUPPORTED = "unsupported"


#: Statuses with a converted document, to chunk.
EXTRACTED = frozenset({ExtractStatus.OK, ExtractStatus.PARTIAL})

#: Statuses that are only tried again when asked to, so that a file that
#: times out doesn't take up its whole timeout on every run.
FAILURES = frozenset({ExtractStatus.FAILED, ExtractStatus.TIMEOUT})

#: Statuses of files that were left out. They have nothing to chunk.
LEFT_OUT = frozenset(
    {
        ExtractStatus.RESTRICTED,
        ExtractStatus.NOT_DOCUMENT,
        ExtractStatus.EMPTY,
        ExtractStatus.UNSUPPORTED,
    }
)

#: The status of each format that's left out.
LEFT_OUT_FORMATS = {
    DocumentFormat.RESTRICTED: ExtractStatus.RESTRICTED,
    DocumentFormat.HTML: ExtractStatus.NOT_DOCUMENT,
    DocumentFormat.EMPTY: ExtractStatus.EMPTY,
    DocumentFormat.UNSUPPORTED: ExtractStatus.UNSUPPORTED,
}


class Device(StrEnum):
    """
    What Docling runs its models on. Docling's own AcceleratorDevice
    isn't used, so that this module imports without Docling.
    """

    AUTO = "auto"
    CPU = "cpu"
    CUDA = "cuda"


@dataclass(frozen=True)
class ConverterSettings:
    """
    How this process's converter is set up. The first conversion in a
    process fixes them: a converter isn't rebuilt for new settings.
    """

    #: Threads for torch and Docling's models.
    threads: int = 4
    #: Give up on a file after this many seconds.
    timeout: float = 1800.0
    device: Device = Device.AUTO


#: Batch sizes for Docling's layout model and TableFormer on a GPU,
#: where batches of pages make better use of it. Docling's default is 4,
#: which is kept on CPU.
GPU_LAYOUT_BATCH_SIZE = 32
GPU_TABLE_BATCH_SIZE = 8


def docling_version() -> str | None:
    """Docling's version, or None if it isn't installed."""
    try:
        import docling  # noqa: F401
    except ImportError:
        return None
    try:
        return importlib.metadata.version("docling")
    except importlib.metadata.PackageNotFoundError:
        return None


def count_pages(data: bytes) -> int | None:
    """
    The number of pages in a PDF, or None if it can't be read or
    pypdfium2 isn't installed.
    """
    try:
        import pypdfium2
    except ImportError:
        return None
    try:
        pdf = pypdfium2.PdfDocument(data)
    except pypdfium2.PdfiumError:
        return None
    try:
        return len(pdf)
    finally:
        pdf.close()


def pdf_title(data: bytes) -> str | None:
    """
    The Title in a PDF's document information, or None if there isn't
    one or the PDF can't be read. Whether it's worth using is decided by
    chunking.document_title().
    """
    try:
        import pypdfium2
    except ImportError:
        return None
    try:
        pdf = pypdfium2.PdfDocument(data)
    except pypdfium2.PdfiumError:
        return None
    try:
        return pdf.get_metadata_value("Title") or None
    except Exception:  # noqa: BLE001
        # A damaged information dictionary. The title isn't worth failing
        # the file over.
        return None
    finally:
        pdf.close()


def document_bytes(data: bytes, document_format: DocumentFormat) -> bytes:
    """
    The bytes to convert: the file's own, the PDF inside it, or a Word
    template as a Word document.
    """
    if document_format == DocumentFormat.MULTIPART_PDF:
        return unwrap_multipart(data)
    if document_format == DocumentFormat.DOCX:
        return as_word_document(data)
    return data


def document_stream(data: bytes, storage_key: str, document_format: DocumentFormat):
    """
    The stream Docling converts, named with the extension of the file's
    format rather than its storage key's. See formats.py.
    """
    from docling_core.types.io import DocumentStream

    return DocumentStream(
        name=stream_name(storage_key, document_format),
        stream=io.BytesIO(data),
    )


@dataclass
class Conversion:
    status: ExtractStatus
    pages: int | None
    error: str | None
    #: The DoclingDocument, if the status is ok or partial.
    document: object | None


def convert(stream) -> Conversion:
    """Convert one stream with this process's converter."""
    from docling.datamodel.base_models import ConversionStatus

    result = converter().convert(stream, raises_on_error=False)
    pages = result.input.page_count or None
    error = "; ".join(e.error_message for e in result.errors) or None
    if result.has_timeout_errors():
        return Conversion(ExtractStatus.TIMEOUT, pages, error, None)
    if result.status == ConversionStatus.SUCCESS:
        return Conversion(ExtractStatus.OK, pages, error, result.document)
    if result.status == ConversionStatus.PARTIAL_SUCCESS:
        return Conversion(ExtractStatus.PARTIAL, pages, error, result.document)
    return Conversion(
        ExtractStatus.FAILED,
        pages,
        error or f"Docling status: {result.status.value}",
        None,
    )


def docling_json(document) -> bytes:
    """
    The DoclingDocument as JSON. The default pipeline doesn't keep
    images, so there are none to save.
    """
    from docling_core.types.doc import ImageRefMode

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "document.json"
        document.save_as_json(path, image_mode=ImageRefMode.PLACEHOLDER)
        return path.read_bytes()


def load_docling_json(data: bytes):
    from docling_core.types.doc import DoclingDocument

    return DoclingDocument.model_validate_json(data)


# The converter in this process


#: Set by configure().
_settings = ConverterSettings()

#: This process's DocumentConverter, made by converter() when it's first
#: needed, so that a process that only chunks doesn't load the models.
_converter = None

#: The device the converter runs on: "cpu" or "cuda".
_device = None


def configure(settings: ConverterSettings):
    """
    Settings for this process's converter. The thread variables have to
    be set before torch is imported, so call this before anything loads
    it. Once the converter is made, calling this again changes nothing.
    """
    global _settings
    _settings = settings
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS"):
        os.environ[name] = str(settings.threads)


def init_worker(settings: ConverterSettings):
    """
    Set up a worker process: limit the threads its libraries start, and
    throw away its output.

    Without the thread limit, several workers each start a thread for
    every core, and run more slowly together than one worker does alone.

    Docling and the models it uses print their own messages: RapidOCR
    four lines each time it loads, and TableFormer warnings such as "1
    of 248 pdf cells matched neither a row nor a column band", some
    through loggers that set their own level. The parent shows progress,
    and status.json has each file's errors, so the workers' output is
    thrown away.
    """
    configure(settings)
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 1)
    os.dup2(devnull, 2)


def converter():
    """
    This process's DocumentConverter. PDFs and images go through
    Docling's default pipeline: its layout model, TableFormer for
    tables, and OCR of regions with no text layer. Office files,
    Markdown and CSV are read without models.
    """
    global _converter, _device
    if _converter is not None:
        return _converter

    import torch
    from docling.datamodel.accelerator_options import AcceleratorOptions
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import (
        DocumentConverter,
        ImageFormatOption,
        PdfFormatOption,
    )

    threads, timeout, device = _settings.threads, _settings.timeout, _settings.device
    torch.set_num_threads(threads)
    if device == Device.AUTO:
        device = Device.CUDA if torch.cuda.is_available() else Device.CPU
    options = PdfPipelineOptions(
        document_timeout=timeout,
        accelerator_options=AcceleratorOptions(num_threads=threads, device=str(device)),
    )
    if device == Device.CUDA:
        options.layout_batch_size = GPU_LAYOUT_BATCH_SIZE
        options.table_batch_size = GPU_TABLE_BATCH_SIZE

    _device = str(device)
    _converter = DocumentConverter(
        allowed_formats=[
            InputFormat.PDF,
            InputFormat.DOCX,
            InputFormat.XLSX,
            InputFormat.PPTX,
            InputFormat.IMAGE,
            InputFormat.MD,
            InputFormat.CSV,
        ],
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=options),
            InputFormat.IMAGE: ImageFormatOption(pipeline_options=options),
        },
    )
    return _converter


def converter_device() -> str | None:
    """The device this process's converter runs on, once it's made."""
    return _device


def worker_pool(workers: int, settings: ConverterSettings) -> ProcessPoolExecutor:
    """
    Worker processes, each with its own converter.

    They're started with spawn rather than fork, so that each imports
    torch afresh after its thread variables are set.
    """
    return ProcessPoolExecutor(
        max_workers=workers,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=init_worker,
        initargs=(settings,),
    )
