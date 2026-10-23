"""
Working out a document's type from its bytes.

The extension is part of a document's storage key, so what each rule
returns is pinned here: changing it means renaming stored files.
"""

import io
import zipfile
from pathlib import Path

import pytest

from lgsf.storage.documents.sniff import (
    HEAD_BYTES,
    needs_whole_file,
    ole2_extension_for_streams,
    sniff_extension,
)

FIXTURES = Path(__file__).parent / "fixtures"

PDF = b"%PDF-1.7\n%\xe2\xe3\xcf\xd3\n1 0 obj\n<< /Type /Catalog >>\nendobj\n"


def make_zip(members, compression=zipfile.ZIP_DEFLATED):
    """A zip holding ``members``, a dict of name to bytes, in that order."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=compression) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def office_open_xml(main_type, part="/word/document.xml"):
    """An Office Open XML file whose main part has this content type."""
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" '
        'ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        f'<Override PartName="{part}" ContentType="{main_type}"/>'
        '<Override PartName="/docProps/core.xml" '
        'ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
        "</Types>"
    )
    return make_zip(
        {
            "[Content_Types].xml": content_types,
            "_rels/.rels": "<Relationships/>",
            part.lstrip("/"): "<document/>",
        }
    )


def opendocument(mimetype):
    # The mimetype member comes first and is stored, as the format says.
    return make_zip(
        {"mimetype": mimetype, "content.xml": "<office:document-content/>"},
        compression=zipfile.ZIP_STORED,
    )


# ---- empty ----


def test_no_bytes_get_no_extension():
    assert sniff_extension(b"") == ""


# ---- zip ----


@pytest.mark.parametrize(
    "main_type,extension",
    [
        (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml",
            ".docx",
        ),
        (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.template.main+xml",
            ".dotx",
        ),
        ("application/vnd.ms-word.document.macroEnabled.main+xml", ".docm"),
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml",
            ".xlsx",
        ),
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml",
            ".xltx",
        ),
        ("application/vnd.ms-excel.sheet.macroEnabled.main+xml", ".xlsm"),
        (
            "application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml",
            ".pptx",
        ),
        (
            "application/vnd.openxmlformats-officedocument.presentationml.slideshow.main+xml",
            ".ppsx",
        ),
    ],
)
def test_office_open_xml_is_told_by_its_main_part(main_type, extension):
    assert sniff_extension(office_open_xml(main_type)) == extension


def test_a_word_template_is_not_a_word_document():
    """36 councils on S3 link .dotx files, and libmagic calls them .docx."""
    document = office_open_xml(
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"
    )
    template = office_open_xml(
        "application/vnd.openxmlformats-officedocument.wordprocessingml.template.main+xml"
    )

    assert sniff_extension(document) == ".docx"
    assert sniff_extension(template) == ".dotx"


@pytest.mark.parametrize(
    "mimetype,extension",
    [
        ("application/vnd.oasis.opendocument.text", ".odt"),
        ("application/vnd.oasis.opendocument.spreadsheet", ".ods"),
        ("application/vnd.oasis.opendocument.presentation", ".odp"),
    ],
)
def test_opendocument_is_told_by_its_mimetype_member(mimetype, extension):
    assert sniff_extension(opendocument(mimetype)) == extension


def test_any_other_zip_is_a_zip():
    assert sniff_extension(make_zip({"notes.txt": "hello"})) == ".zip"


def test_a_zip_of_stored_pdfs_is_a_zip_not_a_pdf():
    """
    Stored without compression, the first PDF's header sits inside the
    zip's first kilobyte (Denbighshire's s46050).
    """
    content = make_zip({"report.pdf": PDF}, compression=zipfile.ZIP_STORED)
    assert b"%PDF-" in content[:1024]

    assert sniff_extension(content) == ".zip"


def test_a_truncated_zip_is_still_a_zip():
    content = office_open_xml(
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"
    )
    assert sniff_extension(content[:100]) == ".zip"


# ---- OLE2 ----


@pytest.mark.parametrize(
    "name,extension",
    [("one-line.doc", ".doc"), ("one-line.xls", ".xls"), ("one-line.ppt", ".ppt")],
)
def test_ole2_files_are_told_by_their_streams(name, extension):
    """Made with LibreOffice from a one-line document."""
    assert sniff_extension((FIXTURES / name).read_bytes()) == extension


@pytest.mark.parametrize(
    "names,extension",
    [
        (["WordDocument", "1Table", "\x05SummaryInformation"], ".doc"),
        (["Workbook"], ".xls"),
        (["Book"], ".xls"),
        (["PowerPoint Document", "Current User"], ".ppt"),
        (["__substg1.0_0037001F", "__properties_version1.0"], ".msg"),
        (["\x05SummaryInformation", "Contents"], ".bin"),
    ],
)
def test_ole2_stream_names_give_the_extension(names, extension):
    assert ole2_extension_for_streams(names) == extension


def test_a_corrupt_ole2_file_is_unidentifiable():
    assert sniff_extension(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 600) == (
        ".bin"
    )


# ---- PDF ----


def test_a_pdf():
    assert sniff_extension(PDF) == ".pdf"


def test_a_pdf_after_a_multipart_header():
    """Neath Port Talbot serves two like this (s103135, s103515), and they open."""
    content = (
        b"--boundary\r\n"
        b'Content-Disposition: form-data; name="file"; filename="report.pdf"\r\n'
        b"Content-Type: application/pdf\r\n\r\n" + PDF
    )
    assert sniff_extension(content) == ".pdf"


def test_a_pdf_header_after_the_first_kilobyte_does_not_count():
    assert sniff_extension(b" " * 1024 + PDF) == ".txt"


# ---- other binary formats ----


@pytest.mark.parametrize(
    "content,extension",
    [
        (b"{\\rtf1\\ansi\\deff0 report}", ".rtf"),
        (b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR", ".png"),
        (b"\xff\xd8\xff\xe0\x00\x10JFIF\x00", ".jpg"),
        (b"GIF87a\x01\x00\x01\x00", ".gif"),
        (b"GIF89a\x01\x00\x01\x00", ".gif"),
        (b"II*\x00\x08\x00\x00\x00", ".tif"),
        (b"MM\x00*\x00\x00\x00\x08", ".tif"),
        (b"RIFF\x24\x08\x00\x00WAVEfmt ", ".wav"),
        (b"ID3\x03\x00\x00\x00\x00\x00\x00", ".mp3"),
        (b"\xff\xfb\x90\x64\x00", ".mp3"),
        (b"\xff\xf3\x90\x64\x00", ".mp3"),
        (b"\xff\xf2\x90\x64\x00", ".mp3"),
        (b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00", ".mp4"),
    ],
)
def test_binary_signatures(content, extension):
    assert sniff_extension(content) == extension


def test_riff_that_is_not_wave_is_not_a_wav():
    assert sniff_extension(b"RIFF\x24\x08\x00\x00AVI LIST\x00") == ".bin"


# ---- HTML and XML ----


@pytest.mark.parametrize(
    "content",
    [
        b"<!DOCTYPE html><html><body>Restricted</body></html>",
        b"<html><head><title>Error</title></head></html>",
        b"<HEAD><TITLE>Error</TITLE></HEAD>",
        b"<body>Not found</body>",
        b"<meta charset='utf-8'><p>Report</p>",
        b"<title>Error</title>",
        # A UTF-8 BOM.
        b"\xef\xbb\xbf<!DOCTYPE html><html></html>",
        # A leading blank line.
        b'\r\n<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN">',
        # Blaenau Gwent's error page.
        b'<link rel="stylesheet" href="/error.css"><div>An error occurred</div>',
        # libmagic calls these JavaScript.
        b"<script>window.location = '/mgError.aspx';</script><html></html>",
        # A comment first.
        b"<!-- Generated by ModernGov -->\n<html></html>",
        # XHTML, as legislation.gov.uk serves it.
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b'<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML+RDFa 1.0//EN" "">'
        b'<html xmlns="http://www.w3.org/1999/xhtml"></html>',
    ],
)
def test_html(content):
    assert sniff_extension(content) == ".html"


def test_xml_that_is_not_html():
    content = b'<?xml version="1.0"?>\n<feed xmlns="http://www.w3.org/2005/Atom"/>'
    assert sniff_extension(content) == ".xml"


def test_xml_after_a_bom():
    content = b'\xef\xbb\xbf<?xml version="1.0"?>\n<rows/>'
    assert sniff_extension(content) == ".xml"


def test_an_unclosed_comment_is_not_html():
    assert sniff_extension(b"<!-- <html>") == ".txt"


# ---- text, and what's left ----


def test_text():
    assert sniff_extension(b"Item,Amount\r\nPaper,12.50\r\n") == ".txt"


def test_text_with_a_bom_and_a_nul_byte_is_text():
    """Waltham Forest serves a text/plain report like this."""
    content = b"\xef\xbb\xbfReport of the Director\r\n\x00\r\nSummary"
    assert sniff_extension(content) == ".txt"


def test_zero_bytes_are_unidentifiable():
    """Pembrokeshire's s64643: 1.6 MB of zero bytes served as a PDF."""
    assert sniff_extension(b"\x00" * 1024) == ".bin"


def test_only_the_first_eight_kilobytes_are_read():
    """So the start of a file gives the same answer as the whole of it."""
    content = b"a" * HEAD_BYTES + b"\x00"
    assert sniff_extension(content) == ".txt"
    assert sniff_extension(content) == sniff_extension(content[:HEAD_BYTES])


# ---- needing the whole file ----


def test_a_zip_needs_the_whole_file():
    assert needs_whole_file(make_zip({"a.txt": "a"})[:HEAD_BYTES])


def test_an_ole2_file_needs_the_whole_file():
    head = (FIXTURES / "one-line.doc").read_bytes()[:HEAD_BYTES]
    assert needs_whole_file(head)


def test_a_pdf_does_not_need_the_whole_file():
    assert not needs_whole_file(PDF)
