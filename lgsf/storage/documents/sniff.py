"""
Working out a document's file type from its bytes.

A council's link says little about what it serves. ModernGov's
``mgConvert2PDF.aspx`` answers with an HTML error page as often as a PDF,
``mgReasonsRestricted.aspx`` pages are HTML, and a link ending ``.pdf`` can
serve a Word file. The response's Content-Type is missing for some
documents. So the extension a stored document gets comes from its bytes.

The extension is part of the document's storage key, so these rules are
part of the key format: if a change here gives some bytes a different
extension, those bytes get a different key, and files already stored under
the old one have to be renamed. The tests pin what each rule returns.

The checks run in order and the first that matches wins. Only the first
``HEAD_BYTES`` are read, except for zip and OLE2 files, whose type is
inside the container and needs the whole file: ``needs_whole_file`` says
which, so a caller holding only the start of a file knows when it has to
fetch the rest.
"""

from __future__ import annotations

import io
import zipfile
from xml.etree import ElementTree

import olefile

#: How much of a document most checks read.
HEAD_BYTES = 8 * 1024

ZIP_MAGIC = b"PK\x03\x04"
OLE2_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
UTF8_BOM = b"\xef\xbb\xbf"

#: The ``mimetype`` member an OpenDocument file starts with.
OPENDOCUMENT_TYPES = {
    "application/vnd.oasis.opendocument.text": ".odt",
    "application/vnd.oasis.opendocument.spreadsheet": ".ods",
    "application/vnd.oasis.opendocument.presentation": ".odp",
}

#: The content type ``[Content_Types].xml`` gives an Office Open XML file's
#: main part.
OFFICE_OPEN_XML_TYPES = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml": ".docx",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.template.main+xml": ".dotx",
    "application/vnd.ms-word.document.macroEnabled.main+xml": ".docm",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml": ".xlsx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml": ".xltx",
    "application/vnd.ms-excel.sheet.macroEnabled.main+xml": ".xlsm",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml": ".pptx",
    "application/vnd.openxmlformats-officedocument.presentationml.slideshow.main+xml": ".ppsx",
}

IMAGE_SIGNATURES = (
    (b"\x89PNG\r\n\x1a\n", ".png"),
    (b"\xff\xd8\xff", ".jpg"),
    (b"GIF87a", ".gif"),
    (b"GIF89a", ".gif"),
    (b"II*\x00", ".tif"),
    (b"MM\x00*", ".tif"),
)

#: An ID3 tag, or the header of an MPEG-1 or MPEG-2 Layer III frame.
MP3_SIGNATURES = (b"ID3", b"\xff\xfb", b"\xff\xf3", b"\xff\xf2")

#: What an HTML page can open with, once any BOM, XML declaration,
#: whitespace and comments are out of the way. Compared lower-cased.
HTML_OPENINGS = (
    b"<!doctype html",
    b"<html",
    b"<head",
    b"<body",
    b"<link",
    b"<meta",
    b"<script",
    b"<title",
)


def needs_whole_file(head: bytes) -> bool:
    """
    True if ``head``, the start of a file, is a zip or OLE2 container,
    whose type can only be told from the whole file.
    """
    return head.startswith((ZIP_MAGIC, OLE2_MAGIC))


def sniff_extension(content: bytes) -> str:
    """
    The extension, with its dot, for a document with these bytes.

    Returns an empty string for no bytes at all, and ``.bin`` for bytes
    none of the rules recognise.
    """
    if not content:
        return ""
    if content.startswith(ZIP_MAGIC):
        return zip_extension(content)
    if content.startswith(OLE2_MAGIC):
        return ole2_extension(content)

    head = content[:HEAD_BYTES]
    # Readers accept a PDF that starts anywhere in its first kilobyte, and
    # councils serve some that do: Neath Port Talbot's come wrapped in a
    # multipart form body.
    if b"%PDF-" in head[:1024]:
        return ".pdf"
    if head.startswith(b"{\\rtf"):
        return ".rtf"
    for signature, extension in IMAGE_SIGNATURES:
        if head.startswith(signature):
            return extension
    if head.startswith(b"RIFF") and head[8:12] == b"WAVE":
        return ".wav"
    if head.startswith(MP3_SIGNATURES):
        return ".mp3"
    if head[4:8] == b"ftyp":
        return ".mp4"

    markup = head.removeprefix(UTF8_BOM)
    if markup_start(markup).startswith(HTML_OPENINGS):
        return ".html"
    if markup.startswith(b"<?xml"):
        return ".xml"

    if head.startswith(UTF8_BOM) or b"\x00" not in head:
        return ".txt"
    return ".bin"


def markup_start(head: bytes) -> bytes:
    """
    ``head`` lower-cased, from its first tag that isn't an XML declaration
    or a comment.

    An HTML page can come after an XML declaration (XHTML), and after
    whitespace or comments: Blaenau Gwent's error page starts with a blank
    line.
    """
    text = head.lower()
    while True:
        text = text.lstrip()
        if text.startswith(b"<?xml"):
            close = b"?>"
        elif text.startswith(b"<!--"):
            close = b"-->"
        else:
            return text
        end = text.find(close)
        if end == -1:
            return b""
        text = text[end + len(close) :]


def zip_extension(content: bytes) -> str:
    """
    ``.odt``, ``.ods`` or ``.odp`` for an OpenDocument file, the Office Open
    XML extension its main part names, or ``.zip``.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            names = set(archive.namelist())
            if "mimetype" in names:
                mimetype = archive.read("mimetype").decode("ascii", "replace").strip()
                if mimetype in OPENDOCUMENT_TYPES:
                    return OPENDOCUMENT_TYPES[mimetype]
            if "[Content_Types].xml" in names:
                types = ElementTree.fromstring(archive.read("[Content_Types].xml"))
                for element in types.iter():
                    extension = OFFICE_OPEN_XML_TYPES.get(element.get("ContentType"))
                    if extension:
                        return extension
    except Exception:
        # A truncated or corrupt zip can fail in many ways, from
        # BadZipFile to struct.error. It is still a zip.
        pass
    return ".zip"


def ole2_extension(content: bytes) -> str:
    """The extension for an OLE2 file, from the streams at its root."""
    try:
        with olefile.OleFileIO(io.BytesIO(content)) as ole:
            names = [
                path[0]
                for path in ole.listdir(streams=True, storages=True)
                if len(path) == 1
            ]
    except Exception:
        # As for zip: olefile has no one exception for a corrupt file.
        return ".bin"
    return ole2_extension_for_streams(names)


def ole2_extension_for_streams(names) -> str:
    """
    The extension for an OLE2 file whose root holds streams and storages
    with these names.

    Only the root is looked at: a Word file holding an embedded spreadsheet
    has a ``Workbook`` stream further down. A Word template (``.dot``) has
    the same streams as a document, so it comes out as ``.doc``.
    """
    names = set(names)
    if "WordDocument" in names:
        return ".doc"
    if "Workbook" in names or "Book" in names:
        return ".xls"
    if "PowerPoint Document" in names:
        return ".ppt"
    if any(name.startswith("__substg1.0_") for name in names):
        return ".msg"
    return ".bin"
