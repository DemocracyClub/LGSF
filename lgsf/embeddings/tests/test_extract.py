"""
Converting files through a DocumentStream, with Docling's own converter.

Only the DOCX and the text email are converted: Docling reads them
without models. PDFs and images need the layout model, TableFormer and
OCR, which aren't loaded in tests. These tests need the embeddings
dependency group and are skipped without it.
"""

from pathlib import Path

import pytest

pytest.importorskip("docling")

from lgsf.embeddings import extract  # noqa: E402
from lgsf.embeddings.chunking import chunk_document, document_title  # noqa: E402
from lgsf.embeddings.extract import ExtractStatus  # noqa: E402
from lgsf.embeddings.formats import DocumentFormat, detect_format  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures" / "formats"
DIGEST = "a" * 64


def convert(path, storage_key):
    document_format = detect_format(path)
    data = extract.document_bytes(path.read_bytes(), document_format)
    stream = extract.document_stream(data, storage_key, document_format)
    return document_format, extract.convert(stream)


def test_docx():
    document_format, conversion = convert(
        FIXTURES / "word-template.docx", f"{DIGEST}.docx"
    )
    assert document_format == DocumentFormat.DOCX
    assert conversion.status == ExtractStatus.OK
    text = conversion.document.export_to_markdown()
    assert "Leadership Strategy in Schools" in text

    title = document_title(conversion.document, None)
    chunks = chunk_document("NTL", f"{DIGEST}.docx", conversion.document, title)
    assert chunks
    assert "Purpose of the Report" in "\n".join(c.text for c in chunks)
    # Word gave the document an empty Title item and no section headings,
    # so there's nothing to head the chunks with.
    assert title is None
    assert chunks[0].header == ""


def test_text_email():
    document_format, conversion = convert(FIXTURES / "email.txt", f"{DIGEST}.txt")
    assert document_format == DocumentFormat.TEXT
    assert conversion.status == ExtractStatus.OK
    assert "Radcliffe Civic Suite" in conversion.document.export_to_markdown()

    chunks = chunk_document("BUR", f"{DIGEST}.txt", conversion.document, None)
    assert "lodge my objection" in "\n".join(c.text for c in chunks)


def test_docling_json_round_trip():
    _, conversion = convert(FIXTURES / "email.txt", f"{DIGEST}.txt")
    data = extract.docling_json(conversion.document)
    assert extract.load_docling_json(data) == conversion.document


def test_count_pages():
    assert extract.count_pages(b"%PDF-1.7 not really") is None
    pdf = extract.document_bytes(
        (FIXTURES / "multipart.pdf").read_bytes(), DocumentFormat.MULTIPART_PDF
    )
    assert extract.count_pages(pdf) == 11


def test_pdf_title():
    assert extract.pdf_title(b"%PDF-1.7 not really") is None
    pdf = extract.document_bytes(
        (FIXTURES / "multipart.pdf").read_bytes(), DocumentFormat.MULTIPART_PDF
    )
    title = extract.pdf_title(pdf)
    assert title is None or isinstance(title, str)
