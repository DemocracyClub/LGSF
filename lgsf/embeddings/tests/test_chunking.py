"""
Chunks and their headers, the document title rule, and the hash that
decides when a document is chunked again.

The chunks of a Manchester decision notice (fixtures/MAN, converted
with Docling 2.130.0) are compared with the expected chunks beside it.
Those tests need docling-core, from the embeddings dependency group.
"""

import json
from pathlib import Path

import pytest

from lgsf.embeddings.chunking import (
    document_title,
    header,
    inputs_hash,
    meaningful_title,
)
from lgsf.embeddings.formats import DocumentFormat

FIXTURES = Path(__file__).parent / "fixtures" / "MAN"
DIGEST = "98e2faaa9eb4bb6ef4228d195741b9e86422805963dd641cc9a4fa72869ecee7"
STORAGE_KEY = f"{DIGEST}.pdf"


@pytest.fixture(scope="module")
def docling_document():
    docling_core = pytest.importorskip("docling_core.types.doc")
    return docling_core.DoclingDocument.load_from_json(
        FIXTURES / "decision-notice.docling.json"
    )


def chunk(docling_document, title="Notice of Decision Made"):
    from lgsf.embeddings.chunking import chunk_document

    return chunk_document("MAN", STORAGE_KEY, docling_document, title)


# The title


@pytest.mark.parametrize(
    ("metadata_title", "expected"),
    [
        ("Report to Cabinet", "Report to Cabinet"),
        ("Microsoft Word - Report to Cabinet.docx", "Report to Cabinet"),
        ("Microsoft Word - Report to Cabinet.doc", "Report to Cabinet"),
        ("  Report   to  Cabinet ", "Report to Cabinet"),
        ("", None),
        (None, None),
        ("untitled", None),
        ("Untitled", None),
        ("Document1", None),
        ("Microsoft Word - Document1", None),
        ("report.pdf", None),
        ("Agenda_final.PDF", None),
        ("Microsoft Word - ", None),
    ],
)
def test_meaningful_title(metadata_title, expected):
    assert meaningful_title(metadata_title) == expected


def test_metadata_title_comes_first(docling_document):
    assert document_title(docling_document, "Decision notice P-1475") == (
        "Decision notice P-1475"
    )


def test_first_heading_when_there_is_no_usable_title(docling_document):
    assert document_title(docling_document, None) == "Decision taken"
    assert document_title(docling_document, "untitled") == "Decision taken"


def test_no_title_without_a_document():
    assert document_title(None, "Microsoft Word - report.pdf") is None


# The header


def test_header_is_title_then_headings():
    assert header("Report to Cabinet", ["Background", "Options"]) == (
        "Report to Cabinet\nBackground · Options"
    )


def test_header_leaves_out_empty_lines():
    assert header("Report to Cabinet", []) == "Report to Cabinet"
    assert header(None, ["Background"]) == "Background"
    assert header(None, []) == ""
    assert header("", ["", "  "]) == ""


def test_header_does_not_repeat_the_title_as_a_heading():
    assert header("Decision taken", ["Decision taken"]) == "Decision taken"
    assert header("Decision taken", ["Decision taken", "Reasons"]) == (
        "Decision taken\nReasons"
    )


# Chunks


def test_chunks_match_the_expected_chunks(docling_document):
    path = FIXTURES / "decision-notice.expected-chunks.jsonl"
    expected = [json.loads(line) for line in path.read_text().splitlines()]
    chunks = chunk(docling_document, title="D3 P-1475 Domestic Abuse")
    assert [c.as_dict() for c in chunks] == expected


def test_chunk_fields(docling_document):
    chunks = chunk(docling_document, title="D3 P-1475 Domestic Abuse")
    assert [c.chunk_id for c in chunks] == [
        f"MAN:documents:{DIGEST}:{index}" for index in range(4)
    ]
    assert {c.council for c in chunks} == {"MAN"}
    assert {c.content_hash for c in chunks} == {f"sha256:{DIGEST}"}
    assert {c.storage_key for c in chunks} == {STORAGE_KEY}
    assert {c.chunk_count for c in chunks} == {4}
    assert chunks[1].header == "D3 P-1475 Domestic Abuse\nNotice of Decision Made"
    assert chunks[1].headings == ["Notice of Decision Made"]
    assert chunks[1].pages == [1, 2]


def test_header_names_no_council_or_date(docling_document):
    chunks = chunk(docling_document, title="D3 P-1475 Domestic Abuse")
    for c in chunks:
        assert "Manchester" not in c.header
        assert "2026" not in c.header


def test_short_chunks_are_not_embedded(docling_document):
    chunks = chunk(docling_document)
    assert [c.embed for c in chunks] == [False, True, True, True]


def test_a_document_with_no_title(docling_document):
    chunks = chunk(docling_document, title=None)
    assert chunks[1].header == "Notice of Decision Made"
    assert chunks[1].embed_text.startswith("Notice of Decision Made\n\n")


def test_as_dict_is_the_schema_fields(docling_document):
    data = chunk(docling_document)[1].as_dict()
    assert sorted(data) == [
        "chunk_count",
        "chunk_id",
        "chunk_index",
        "chunker_version",
        "content_hash",
        "council",
        "embed",
        "header",
        "headings",
        "pages",
        "storage_key",
        "text",
        "text_hash",
        "word_count",
    ]


# inputs_hash()


def man_hash(**kwargs):
    return inputs_hash(DocumentFormat.PDF, "2.130.0", f"sha256:{DIGEST}", **kwargs)


def test_inputs_hash_is_stable():
    assert man_hash() == man_hash()
    assert man_hash().startswith("sha256:")


def test_inputs_hash_changes_with_the_settings_and_versions():
    assert man_hash(max_words=300) != man_hash()
    assert man_hash(min_embed_words=10) != man_hash()
    assert inputs_hash(DocumentFormat.PDF, "2.131.0", f"sha256:{DIGEST}") != man_hash()
    assert inputs_hash(DocumentFormat.DOCX, "2.130.0", f"sha256:{DIGEST}") != man_hash()
    assert (
        inputs_hash(DocumentFormat.PDF, "2.130.0", "sha256:" + "b" * 64) != man_hash()
    )


def test_inputs_hash_changes_with_chunker_version(monkeypatch):
    before = man_hash()
    monkeypatch.setattr("lgsf.embeddings.records.CHUNKER_VERSION", 999)
    assert man_hash() != before
