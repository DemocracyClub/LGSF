"""
detect_format(), against files copied from councils' documents where
there's one to copy, and made up where there isn't.
"""

import io
import zipfile
from pathlib import Path

import pytest

from lgsf.embeddings.formats import (
    DocumentFormat,
    as_word_document,
    detect_format,
    stream_name,
    unwrap_multipart,
)

FIXTURES = Path(__file__).parent / "fixtures" / "formats"
MULTIPART_PDF = FIXTURES / "multipart.pdf"


@pytest.mark.parametrize(
    "name",
    [
        # Each council's page has its own markup around the heading.
        "restricted-flintshire.html",
        "restricted-oldham.html",
        "restricted-anglesey.html",
        "restricted-salford.html",
        # The heading is an <h3>.
        "restricted-trafford.html",
    ],
)
def test_restricted_pages(name):
    assert detect_format(FIXTURES / name) == DocumentFormat.RESTRICTED


def test_other_html_pages():
    # A Tameside logon page, saved in place of a report.
    assert detect_format(FIXTURES / "logon-tameside.html") == DocumentFormat.HTML


def test_empty_file(tmp_path):
    path = tmp_path / "a.pdf"
    path.write_bytes(b"")
    assert detect_format(path) == DocumentFormat.EMPTY


def test_file_of_zero_bytes(tmp_path):
    path = tmp_path / "a.pdf"
    path.write_bytes(b"\x00" * 100_000)
    assert detect_format(path) == DocumentFormat.EMPTY


def test_zero_bytes_then_something(tmp_path):
    path = tmp_path / "a.pdf"
    path.write_bytes(b"\x00" * 100_000 + b"\x01")
    assert detect_format(path) == DocumentFormat.UNSUPPORTED


def test_pdf(tmp_path):
    path = tmp_path / "a.pdf"
    path.write_bytes(b"%PDF-1.7\n")
    assert detect_format(path) == DocumentFormat.PDF


def test_multipart_pdf():
    assert detect_format(MULTIPART_PDF) == DocumentFormat.MULTIPART_PDF


def test_unwrap_multipart():
    pdf = unwrap_multipart(MULTIPART_PDF.read_bytes())
    assert pdf.startswith(b"%PDF-1.5\r\n")
    assert pdf.endswith(b"%%EOF\r\n")


def test_unwrap_multipart_without_a_closing_boundary():
    data = MULTIPART_PDF.read_bytes()
    with pytest.raises(ValueError):
        unwrap_multipart(data[: len(data) // 2])


def test_docx():
    assert detect_format(FIXTURES / "word-template.docx") == DocumentFormat.DOCX


def test_a_word_template_as_a_document():
    data = (FIXTURES / "word-template.docx").read_bytes()
    converted = zipfile.ZipFile(io.BytesIO(as_word_document(data)))
    original = zipfile.ZipFile(io.BytesIO(data))
    content_types = converted.read("[Content_Types].xml")
    assert b"wordprocessingml.document.main+xml" in content_types
    assert b"wordprocessingml.template.main+xml" not in content_types
    assert converted.namelist() == original.namelist()
    assert converted.read("word/document.xml") == original.read("word/document.xml")


def test_a_word_document_is_left_alone():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("[Content_Types].xml", b"<Types/>")
        archive.writestr("word/document.xml", b"<document/>")
    assert as_word_document(output.getvalue()) == output.getvalue()


@pytest.mark.parametrize(
    ("member", "document_format"),
    [
        ("xl/workbook.xml", DocumentFormat.XLSX),
        ("ppt/presentation.xml", DocumentFormat.PPTX),
        # A ZIP of traffic order plans
        ("Moel Famau TRO_Plans/10_BB95.pdf", DocumentFormat.UNSUPPORTED),
    ],
)
def test_other_zips(tmp_path, member, document_format):
    path = tmp_path / "a.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, b"")
    assert detect_format(path) == document_format


def test_text_email():
    assert detect_format(FIXTURES / "email.txt") == DocumentFormat.TEXT


def test_csv(tmp_path):
    path = tmp_path / "a.csv"
    path.write_bytes(b"Applicant,Amount\r\nSeaton Hall,1395\r\n")
    assert detect_format(path) == DocumentFormat.CSV


@pytest.mark.parametrize(
    "signature",
    [
        b"\x89PNG\r\n\x1a\n",
        b"\xff\xd8\xff\xe0",
        b"II*\x00",
        b"RIFF\0\0\0\0WEBP",
    ],
)
def test_images(tmp_path, signature):
    path = tmp_path / "a.png"
    path.write_bytes(signature + b"\x00\x01\x02")
    assert detect_format(path) == DocumentFormat.IMAGE


def test_rtf_is_unsupported(tmp_path):
    path = tmp_path / "a.rtf"
    path.write_bytes(b"{\\rtf1\\ansi\\deff0 {\\fonttbl {\\f0 Times;}} Hello}")
    assert detect_format(path) == DocumentFormat.UNSUPPORTED


def test_other_binary_files(tmp_path):
    # An old Word document
    path = tmp_path / "a.doc"
    path.write_bytes(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1\x00\x00")
    assert detect_format(path) == DocumentFormat.UNSUPPORTED


DIGEST = "a" * 64


def test_stream_name():
    assert stream_name(f"{DIGEST}.txt", DocumentFormat.TEXT) == f"{DIGEST}.md"
    assert stream_name(f"{DIGEST}.docx", DocumentFormat.DOCX) == f"{DIGEST}.docx"
    assert stream_name(f"{DIGEST}.pdf", DocumentFormat.MULTIPART_PDF) == f"{DIGEST}.pdf"


def test_left_out_formats_have_no_stream_name():
    with pytest.raises(KeyError):
        stream_name(f"{DIGEST}.html", DocumentFormat.RESTRICTED)
