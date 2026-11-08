"""
Working out what a document's file is from its bytes.

A stored document's key ends with the extension its bytes were sniffed
as (lgsf.storage.documents.sniff), but what the pipeline does with a
file depends on finer distinctions than that: a PDF wrapped in a
multipart form body, ModernGov's "Reasons restricted" page saved in
place of a document the council hasn't published, other HTML pages
saved in place of one (logon pages, error pages), empty files, Word
templates, ZIPs of PDFs. Those are told apart here.

Docling chooses a backend from a file's bytes, and from its extension
when the bytes don't identify it. So a file is given to Docling as a
stream named with the extension of the format found here: see
stream_name().
"""

import io
import re
import zipfile
from enum import StrEnum
from pathlib import Path


class DocumentFormat(StrEnum):
    PDF = "pdf"
    #: A PDF that was saved with the multipart/form-data body it was sent
    #: in. unwrap_multipart() takes the PDF out.
    MULTIPART_PDF = "multipart_pdf"
    DOCX = "docx"
    XLSX = "xlsx"
    PPTX = "pptx"
    #: PNG, JPEG, TIFF or WebP.
    IMAGE = "image"
    #: Plain text, such as an email saved as text. Converted as Markdown.
    TEXT = "text"
    #: Text in a file whose key ends ".csv".
    CSV = "csv"
    #: ModernGov's "Reasons restricted" page.
    RESTRICTED = "restricted"
    #: Any other HTML page.
    HTML = "html"
    #: No bytes, or only zero bytes.
    EMPTY = "empty"
    #: Anything else, such as a ZIP that isn't an Office document.
    UNSUPPORTED = "unsupported"


#: The extension a stream of each format that's converted is named with,
#: and so the backend Docling uses for it. Docling recognises an image
#: from its bytes before it looks at the name, so any image extension
#: will do.
EXTENSIONS = {
    DocumentFormat.PDF: "pdf",
    DocumentFormat.MULTIPART_PDF: "pdf",
    DocumentFormat.DOCX: "docx",
    DocumentFormat.XLSX: "xlsx",
    DocumentFormat.PPTX: "pptx",
    DocumentFormat.IMAGE: "png",
    DocumentFormat.TEXT: "md",
    DocumentFormat.CSV: "csv",
}

#: Formats sent to Docling. The others are left out without loading it.
CONVERTED = frozenset(EXTENSIONS)

#: Formats whose pages can be counted before converting.
PDF_FORMATS = frozenset({DocumentFormat.PDF, DocumentFormat.MULTIPART_PDF})

#: How much of a file is read to tell what it is.
HEAD_SIZE = 8192

_IMAGE_SIGNATURES = (
    b"\x89PNG\r\n\x1a\n",
    b"\xff\xd8\xff",  # JPEG
    b"II*\x00",  # TIFF, little-endian
    b"MM\x00*",  # TIFF, big-endian
)

#: The members that tell an Office document from any other ZIP.
_OFFICE_MEMBERS = {
    "word/document.xml": DocumentFormat.DOCX,
    "xl/workbook.xml": DocumentFormat.XLSX,
    "ppt/presentation.xml": DocumentFormat.PPTX,
}

#: The heading of ModernGov's restricted page. Each council's page has
#: its own layout around it, and most have the heading as
#: `<h1>Reasons restricted</h1>` or
#: `<h1  class="mgMainTitleTxt" >Reasons restricted</h1>`. BGE's is an
#: <h2> and TRF's an <h3>.
_RESTRICTED_HEADING = re.compile(rb"<h[1-6][^>]*>\s*Reasons restricted", re.IGNORECASE)

#: Control characters that don't appear in text. Tabs, line breaks and
#: form feeds do.
_BINARY_BYTES = re.compile(rb"[\x00-\x08\x0b\x0e-\x1f\x7f]")


def detect_format(path: Path) -> DocumentFormat:
    with path.open("rb") as f:
        head = f.read(HEAD_SIZE)

    if _is_empty(path, head):
        return DocumentFormat.EMPTY
    if head.startswith(b"%PDF-"):
        return DocumentFormat.PDF
    if _is_multipart_pdf(head):
        return DocumentFormat.MULTIPART_PDF
    if head.startswith(b"PK\x03\x04"):
        return _zip_format(path)
    if head.startswith(_IMAGE_SIGNATURES) or (
        head.startswith(b"RIFF") and head[8:12] == b"WEBP"
    ):
        return DocumentFormat.IMAGE
    if _is_html(head):
        if _RESTRICTED_HEADING.search(path.read_bytes()):
            return DocumentFormat.RESTRICTED
        return DocumentFormat.HTML
    if head.startswith(b"{\\rtf"):
        # Rich Text is all printable bytes, but isn't text to read as it
        # is, and Docling has no backend for it.
        return DocumentFormat.UNSUPPORTED
    if not _BINARY_BYTES.search(head):
        if path.suffix.lower() == ".csv":
            return DocumentFormat.CSV
        return DocumentFormat.TEXT
    return DocumentFormat.UNSUPPORTED


def _is_empty(path: Path, head: bytes) -> bool:
    if head.strip(b"\x00"):
        return False
    # The first HEAD_SIZE bytes are zero. Check the rest, a block at a time.
    with path.open("rb") as f:
        while block := f.read(1024 * 1024):
            if block.strip(b"\x00"):
                return False
    return True


def _is_multipart_pdf(head: bytes) -> bool:
    """
    The body starts with a boundary line, then the part's headers, then a
    blank line before the PDF:

        --EUx9zqaqn2TsxFgBzOaJzS6z_2kK5ELVkgBcO2R
        Content-Disposition: form-data; name="data"; filename="....pdf"
        Content-Type: application/octet-stream
        Content-Transfer-Encoding: binary

        %PDF-1.5
    """
    if not head.startswith(b"--"):
        return False
    headers, separator, body = head.partition(b"\r\n\r\n")
    return (
        bool(separator)
        and b"content-disposition: form-data" in headers.lower()
        and body.startswith(b"%PDF-")
    )


def unwrap_multipart(data: bytes) -> bytes:
    """The first part of a multipart/form-data body."""
    boundary, _, rest = data.partition(b"\r\n")
    _, separator, rest = rest.partition(b"\r\n\r\n")
    if not boundary.startswith(b"--") or not separator:
        raise ValueError("Not a multipart body")
    part, separator, _ = rest.partition(b"\r\n" + boundary)
    if not separator:
        raise ValueError("The multipart body has no closing boundary")
    return part


#: The content types of a Word template's main part, and of a Word
#: document's.
_WORD_TEMPLATE = (
    b"application/vnd.openxmlformats-officedocument.wordprocessingml.template.main+xml"
)
_WORD_DOCUMENT = (
    b"application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"
)


def as_word_document(data: bytes) -> bytes:
    """
    A DOCX, with its content type changed from a Word template's to a
    Word document's if it's a template. Many councils serve Word files
    saved as templates. python-docx, which Docling reads them with,
    won't open a template, and the parts are the same in each.
    """
    source = zipfile.ZipFile(io.BytesIO(data))
    content_types = source.read("[Content_Types].xml")
    if _WORD_TEMPLATE not in content_types:
        return data
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as target:
        for member in source.infolist():
            contents = source.read(member)
            if member.filename == "[Content_Types].xml":
                contents = contents.replace(_WORD_TEMPLATE, _WORD_DOCUMENT)
            target.writestr(member, contents)
    return output.getvalue()


def _zip_format(path: Path) -> DocumentFormat:
    try:
        with zipfile.ZipFile(path) as archive:
            names = set(archive.namelist())
    except zipfile.BadZipFile:
        return DocumentFormat.UNSUPPORTED
    for member, document_format in _OFFICE_MEMBERS.items():
        if member in names:
            return document_format
    return DocumentFormat.UNSUPPORTED


def _is_html(head: bytes) -> bool:
    start = head.lstrip(b"\xef\xbb\xbf \t\r\n").lower()
    return start.startswith((b"<!doctype html", b"<html")) or (
        b"<html" in head[:1024].lower()
    )


def stream_name(storage_key: str, document_format: DocumentFormat) -> str:
    """
    The name a converted file's stream is given: its storage key with the
    extension of its format, such as ``<sha256>.md`` for a text email
    stored as ``<sha256>.txt``.
    """
    return f"{Path(storage_key).stem}.{EXTENSIONS[document_format]}"
