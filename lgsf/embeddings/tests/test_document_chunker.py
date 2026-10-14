"""
DocumentChunker, and the table serializer it uses, against a real MAN
decision notice: D3 P-1475, linked from decision 1172 of 6 August 2026,
as `manage.py embeddings extract` saved it with Docling 2.130.0.

It's two pages of tables of labels and values, with no header rows, in
four sections.

MAN's other fixture decisions link to documents that weren't extracted.
These tests need docling-core, from the embeddings dependency group, and
are skipped without it.
"""

import datetime
import json
import shutil
from pathlib import Path

import pytest

pytest.importorskip("docling_core")

import pandas as pd  # noqa: E402

from lgsf.embeddings.chunkers import DocumentChunker  # noqa: E402
from lgsf.embeddings.documents import table_lines  # noqa: E402
from lgsf.embeddings.extract import ExtractionLog, extracted_path  # noqa: E402
from lgsf.embeddings.models import ChunkType, SourceType  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
CONTENT_HASH = "sha256:98e2faaa9eb4bb6ef4228d195741b9e86422805963dd641cc9a4fa72869ecee7"
PARENT_ID = (
    "MAN:documents:98e2faaa9eb4bb6ef4228d195741b9e86422805963dd641cc9a4fa72869ecee7"
)


@pytest.fixture
def data_path(tmp_path):
    path = tmp_path / "MAN"
    shutil.copytree(FIXTURES / "MAN", path)
    return path


def get_chunks(data_path, **dates):
    chunker = DocumentChunker("MAN", data_path=data_path, **dates)
    return chunker.get_chunks(), chunker


def test_chunks_and_ids(data_path):
    chunks, chunker = get_chunks(data_path)
    assert chunker.sources_read == 1
    assert [c.chunk_id for c in chunks] == [f"{PARENT_ID}:{i}" for i in range(4)]
    assert {c.parent_id for c in chunks} == {PARENT_ID}
    assert {c.chunk_count for c in chunks} == {4}
    assert {c.type for c in chunks} == {ChunkType.DOCUMENTS}
    assert {c.source_type for c in chunks} == {SourceType.PDF_DOCLING}


def test_context_header(data_path):
    chunks, _ = get_chunks(data_path)
    assert chunks[1].date == "2026-08-06"
    assert chunks[1].context == (
        "Manchester City Council · Strategic Director (Corporate Services) "
        "& City Treasurer · 6 August 2026\n"
        "Decision: P-1475 Domestic Abuse - Intersecting Needs Safe "
        "Accommodation (2026/06/09A)\n"
        "D3 P-1475 Domestic Abuse - Intersecting Needs Safe Accommodation"
        " · Notice of Decision Made"
    )


def test_tables_with_no_header_row_are_labels_and_values(data_path):
    chunks, _ = get_chunks(data_path)
    lines = chunks[1].text.split("\n")
    assert lines[:2] == [
        "Decision maker: City Treasurer and Interim Deputy Chief Executive",
        "Date of decision: 6 August 2026",
    ]
    assert ", 1 =" not in "\n".join(c.text for c in chunks)


def test_metadata(data_path):
    chunks, _ = get_chunks(data_path)
    metadata = chunks[1].metadata
    assert metadata["url"] == (
        "https://democracy.manchester.gov.uk/documents/s65106/D3 P-1475 Domestic "
        "Abuse - Intersecting Needs Safe Accommodation.pdf"
    )
    assert metadata["content_hash"] == CONTENT_HASH
    assert metadata["storage_key"] == "2026-08-06-1172-s65106.pdf"
    assert metadata["document_title"] == (
        "D3 P-1475 Domestic Abuse - Intersecting Needs Safe Accommodation"
    )
    assert metadata["pages"] == [1, 2]
    assert metadata["headings"] == ["Notice of Decision Made"]
    assert metadata["decisions"] == [
        {
            "identifier": "1172",
            "title": "P-1475 Domestic Abuse - Intersecting Needs Safe "
            "Accommodation (2026/06/09A)",
            "url": "https://democracy.manchester.gov.uk/ieDecisionDetails.aspx?ID=1172",
            "date": "2026-08-06",
        }
    ]


def test_short_chunks_are_not_embedded(data_path):
    chunks, _ = get_chunks(data_path)
    assert chunks[0].text == "Made by an Executive Member or Chief Officer"
    assert [c.embed for c in chunks] == [False, True, True, True]


def test_a_document_linked_from_several_decisions(data_path):
    # A later decision linking to the same file under another key.
    decision = json.loads(
        (data_path / "Decisions" / "json" / "2026-08-06-1172.json").read_text()
    )
    decision.update(
        {"raw_identifier": "1200", "raw_date": "2026-09-01", "raw_title": "Later"}
    )
    decision["documents"][0]["storage_key"] = "2026-09-01-1200-s70000.pdf"
    (data_path / "Decisions" / "json" / "2026-09-01-1200.json").write_text(
        json.dumps(decision)
    )
    chunks, _ = get_chunks(data_path)
    # The header names the earliest decision, and the metadata lists both.
    assert "6 August 2026" in chunks[1].context
    assert [d["identifier"] for d in chunks[1].metadata["decisions"]] == [
        "1172",
        "1200",
    ]

    # Without the first decision in range, it's the later one's.
    chunks, _ = get_chunks(data_path, since=datetime.date(2026, 8, 7))
    assert chunks[1].date == "2026-09-01"
    assert chunks[1].context.split("\n")[1] == "Decision: Later"
    assert chunks[1].metadata["storage_key"] == "2026-09-01-1200-s70000.pdf"


def test_date_range(data_path):
    chunks, chunker = get_chunks(data_path, until=datetime.date(2026, 8, 5))
    assert chunks == []
    assert chunker.sources_read == 0


def test_documents_without_text_are_counted(data_path):
    _, chunker = get_chunks(data_path, since=datetime.date(2026, 1, 1))
    # Decision 1109 links to six documents that weren't extracted.
    assert chunker.not_chunked == {"not extracted": 6}

    log = ExtractionLog(extracted_path(data_path))
    log.append(
        {
            "content_hash": CONTENT_HASH,
            "storage_key": "2026-08-06-1172-s65106.pdf",
            "file": None,
            "status": "failed",
        }
    )
    chunks, chunker = get_chunks(data_path, since=datetime.date(2026, 1, 1))
    # The last line counts.
    assert chunks == []
    assert chunker.not_chunked == {"not extracted": 6, "failed": 1}


def test_has_data(data_path, tmp_path):
    assert DocumentChunker("MAN", data_path=data_path).has_data()
    (data_path / "Embeddings" / "extracted" / "log.jsonl").unlink()
    assert not DocumentChunker("MAN", data_path=data_path).has_data()


def test_table_with_no_header_row():
    table = pd.DataFrame(
        [
            ["Decision maker", "City Treasurer"],
            ["Spans both columns", "Spans both columns"],
            ["Not filled in", ""],
            ["Two values", "One"],
        ]
    )
    table[2] = ["", "", "", "Two"]
    assert table_lines(table) == [
        "Decision maker: City Treasurer",
        "Spans both columns",
        "Not filled in",
        "Two values: One | Two",
    ]


def test_table_with_a_header_row():
    table = pd.DataFrame(
        [["Axewoods Co-Op", "£1,395", "Agreed"], ["Seaton Hall", "", "Deferred"]],
        columns=["Applicant", "Amount", "Decision"],
    )
    assert table_lines(table) == [
        "Axewoods Co-Op, Amount = £1,395. Axewoods Co-Op, Decision = Agreed",
        "Seaton Hall, Decision = Deferred",
    ]


def test_table_with_only_a_header_row():
    table = pd.DataFrame([], columns=["Applicant", "Amount"])
    assert table_lines(table) == ["Applicant | Amount"]
