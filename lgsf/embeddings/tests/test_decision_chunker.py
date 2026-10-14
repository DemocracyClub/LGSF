"""
DecisionChunker against decisions as the decisions scraper stored them:

- MAN 1172 (6 August 2026), with both a purpose and text
- MAN 1109 (11 February 2026), with neither
- MAN 1093 (30 December 2025), before 2026
- PEM 3064 (12 January 2026), "Minutes of Previous Meeting", taken at a
  meeting, so its decision maker starts with the meeting's date
- PEM 3256 (16 March 2026), long enough to be split
"""

import datetime
import shutil
from pathlib import Path

import pytest

from lgsf.embeddings.chunkers import DecisionChunker
from lgsf.embeddings.models import ChunkType, SourceType

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def data_dir(tmp_path):
    for council in ("MAN", "PEM"):
        shutil.copytree(
            FIXTURES / council / "Decisions", tmp_path / council / "Decisions"
        )
    return tmp_path


def get_chunks(data_dir, council, **dates):
    chunker = DecisionChunker(council, data_path=data_dir / council, **dates)
    return chunker.get_chunks(), chunker


def by_decision(chunks):
    decisions = {}
    for chunk in chunks:
        decisions.setdefault(chunk.parent_id, []).append(chunk)
    return decisions


def test_one_source_per_decision(data_dir):
    chunks, chunker = get_chunks(data_dir, "MAN")
    assert chunker.sources_read == 3
    assert list(by_decision(chunks)) == [
        "MAN:decisions:1093",
        "MAN:decisions:1109",
        "MAN:decisions:1172",
    ]
    assert {c.type for c in chunks} == {ChunkType.DECISIONS}
    assert {c.source_type for c in chunks} == {SourceType.DECISION_JSON}


def test_ids_of_a_split_decision(data_dir):
    chunks, _ = get_chunks(data_dir, "PEM")
    split = by_decision(chunks)["PEM:decisions:3256"]
    assert len(split) > 1
    assert [c.chunk_id for c in split] == [
        f"PEM:decisions:3256:{i}" for i in range(len(split))
    ]
    assert all(c.chunk_count == len(split) for c in split)
    assert all(c.word_count <= 350 for c in split)


def test_date_range(data_dir):
    chunks, chunker = get_chunks(data_dir, "MAN", since=datetime.date(2026, 1, 1))
    assert chunker.sources_read == 2
    assert list(by_decision(chunks)) == ["MAN:decisions:1109", "MAN:decisions:1172"]
    chunks, _ = get_chunks(
        data_dir,
        "MAN",
        since=datetime.date(2026, 1, 1),
        until=datetime.date(2026, 8, 5),
    )
    assert list(by_decision(chunks)) == ["MAN:decisions:1109"]


def test_context_header(data_dir):
    chunks, _ = get_chunks(data_dir, "MAN")
    [chunk] = by_decision(chunks)["MAN:decisions:1172"]
    assert chunk.date == "2026-08-06"
    assert chunk.context == (
        "Manchester City Council · Strategic Director (Corporate Services) "
        "& City Treasurer · 6 August 2026\n"
        "Decision: P-1475 Domestic Abuse - Intersecting Needs Safe "
        "Accommodation (2026/06/09A)"
    )


def test_meeting_date_is_left_out_of_the_header(data_dir):
    chunks, _ = get_chunks(data_dir, "PEM")
    [chunk] = by_decision(chunks)["PEM:decisions:3064"]
    assert chunk.context.split("\n")[0] == (
        "Pembrokeshire County Council · Cabinet · 12 January 2026"
    )
    assert chunk.metadata["decision_maker"] == "12/01/2026 - Cabinet"


def test_purpose_then_text(data_dir):
    chunks, _ = get_chunks(data_dir, "MAN")
    [chunk] = by_decision(chunks)["MAN:decisions:1172"]
    paragraphs = chunk.text.split("\n\n")
    assert paragraphs[0].startswith("A contract for the provision of safe")
    assert paragraphs[1].startswith("The appointment of a provider")
    assert len(paragraphs) == 3


def test_decision_with_no_purpose_or_text_is_its_header(data_dir):
    chunks, _ = get_chunks(data_dir, "MAN")
    [chunk] = by_decision(chunks)["MAN:decisions:1109"]
    assert chunk.text == ""
    assert chunk.embed
    assert chunk.embed_text == (
        "Manchester City Council · Executive · 11 February 2026\n"
        "Decision: Adult Social Care Budget 2026/27 - 2028/29"
    )


def test_procedural_decisions_are_not_embedded(data_dir):
    chunks, _ = get_chunks(data_dir, "PEM")
    embed = {parent_id: c[0].embed for parent_id, c in by_decision(chunks).items()}
    assert embed == {"PEM:decisions:3064": False, "PEM:decisions:3256": True}


@pytest.mark.parametrize(
    "title",
    [
        "Apologies for Absence",
        "Declarations of Interest (Including Whipping Declarations)",
        "Disclosures of Personal & Prejudicial Interests.",
        "Minutes of the previous meeting",
        "3. Minutes",
        "Urgent Matters as Agreed by the Chair",
        "Any items that the Chair has decided are urgent",
    ],
)
def test_procedural_titles(data_dir, title):
    chunker = DecisionChunker("MAN", data_path=data_dir / "MAN")
    decision = {"raw_identifier": "1", "raw_title": title, "text": "None."}
    assert not chunker.chunks_for_decision(decision)[0].embed


@pytest.mark.parametrize(
    "title",
    ["Urgent Repairs to the Town Hall", "Minutes Silence Policy", "Interest Rates"],
)
def test_titles_that_arent_procedural(data_dir, title):
    chunker = DecisionChunker("MAN", data_path=data_dir / "MAN")
    decision = {"raw_identifier": "1", "raw_title": title, "text": "None."}
    assert chunker.chunks_for_decision(decision)[0].embed


def test_metadata(data_dir):
    chunks, _ = get_chunks(data_dir, "MAN")
    [chunk] = by_decision(chunks)["MAN:decisions:1172"]
    assert chunk.metadata == {
        "url": "https://democracy.manchester.gov.uk/ieDecisionDetails.aspx?ID=1172",
        "title": "P-1475 Domestic Abuse - Intersecting Needs Safe Accommodation "
        "(2026/06/09A)",
        "decision_maker": "Strategic Director (Corporate Services) & City Treasurer",
        "status": "Recommendations Approved",
        "publication_date": "2026-08-19",
        "is_key_decision": True,
        "is_officer_decision": True,
        "is_subject_to_call_in": True,
        "document_hashes": [
            "sha256:98e2faaa9eb4bb6ef4228d195741b9e86422805963dd641cc9a4fa72869ecee7"
        ],
    }
    [no_text] = by_decision(chunks)["MAN:decisions:1109"]
    assert len(no_text.metadata["document_hashes"]) == 6


def test_has_data(data_dir, tmp_path):
    assert DecisionChunker("MAN", data_path=data_dir / "MAN").has_data()
    assert not DecisionChunker("MAN", data_path=tmp_path / "empty").has_data()


def test_written_to_decisions_jsonl(data_dir):
    chunks, chunker = get_chunks(data_dir, "MAN")
    assert chunker.write(chunks) == data_dir / "MAN" / "Embeddings" / "decisions.jsonl"
