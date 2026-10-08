"""
ModGovMinutesChunker against a real EDE meeting: the Community Grant Panel
of 30 March 2026, as the minutes scraper stored it.

Its agenda items cover what the chunker has to handle: a declarations
item and three procedural items that each say only "None.", and one
long item with a table, RESOLVED blocks and Word-numbered lists that's
split into several chunks.
"""

import json
import shutil
from pathlib import Path

import pytest

from lgsf.embeddings.chunkers import ModGovMinutesChunker
from lgsf.embeddings.models import Chunk

FIXTURES = Path(__file__).parent / "fixtures"
MEETING = "2026-03-30-2681-community-grant-panel"


@pytest.fixture
def data_path(tmp_path):
    path = tmp_path / "EDE"
    shutil.copytree(FIXTURES / "EDE", path)
    return path


@pytest.fixture
def chunks(data_path):
    return ModGovMinutesChunker("EDE", data_path=data_path).get_chunks()


def by_item(chunks):
    items = {}
    for chunk in chunks:
        items.setdefault(chunk.metadata["agenda_item_id"], []).append(chunk)
    return items


def test_one_chunk_per_short_item_and_several_for_the_long_one(chunks):
    items = by_item(chunks)
    assert list(items) == ["17537", "17538", "17539", "17540", "17543"]
    for item_id in ["17537", "17538", "17539", "17540"]:
        assert len(items[item_id]) == 1
        assert items[item_id][0].text == "None."
    long_item = items["17543"]
    assert len(long_item) > 1
    assert all(c.chunk_count == len(long_item) for c in long_item)
    assert [c.chunk_index for c in long_item] == list(range(len(long_item)))
    assert all(c.word_count <= 350 for c in long_item)


def test_ids(chunks):
    items = by_item(chunks)
    first = items["17543"][0]
    assert first.parent_id == "EDE:minutes:2681:17543"
    assert first.chunk_id == "EDE:minutes:2681:17543:0"
    assert items["17543"][1].chunk_id == "EDE:minutes:2681:17543:1"
    assert len({c.chunk_id for c in chunks}) == len(chunks)


def test_minute_numbers_and_metadata(chunks):
    items = by_item(chunks)
    assert [items[i][0].metadata["minute_number"] for i in items] == [
        "6",
        "7",
        "8",
        "9",
        "10",
    ]
    metadata = items["17543"][0].metadata
    assert metadata["committee"] == "Community Grant Panel"
    assert metadata["committee_id"] == "207"
    assert metadata["meeting_id"] == "2681"
    assert metadata["agenda_item_title"] == "Community Grant Funding Awards 2025/26"
    assert metadata["restricted"] is False
    assert metadata["meeting_url"] == (
        "https://democracy.eastdevon.gov.uk/ieListDocuments.aspx?CId=207&MId=2681"
    )
    assert metadata["linked_attachment_ids"][:2] == ["30490", "30492"]
    assert len(metadata["linked_attachment_ids"]) == 11


def test_context_header(chunks):
    chunk = by_item(chunks)["17543"][0]
    assert chunk.date == "2026-03-30"
    assert chunk.context == (
        "East Devon District Council · Community Grant Panel · 30 March 2026\n"
        "Minute 10: Community Grant Funding Awards 2025/26"
    )


def test_text_keeps_list_numbers_and_table_rows(chunks):
    text = "\n\n".join(c.text for c in by_item(chunks)["17543"])
    assert "1. that the remaining four applications" in text
    assert "Axewoods Co-Op | East Devon | £1,395 | Agreed" in text.split("\n")


def test_embed_flag(chunks):
    embed = {item_id: c[0].embed for item_id, c in by_item(chunks).items()}
    assert embed == {
        # "Declarations of Interest": "None." is still worth finding
        "17537": True,
        "17538": False,
        "17539": False,
        "17540": False,
        "17543": True,
    }


def test_meetings_without_minutes_are_skipped(data_path):
    json_dir = data_path / "Minutes" / "json"
    raw_dir = data_path / "Minutes" / "raw"
    meeting = json.loads((json_dir / f"{MEETING}.json").read_text())
    meeting["minutes_published"] = False
    (json_dir / "unpublished.json").write_text(json.dumps(meeting))
    shutil.copy(raw_dir / f"{MEETING}.xml", raw_dir / "unpublished.xml")

    chunker = ModGovMinutesChunker("EDE", data_path=data_path)
    chunks = chunker.get_chunks()
    assert chunker.sources_read == 1
    assert len(by_item(chunks)) == 5


def test_written_output_is_the_same_across_runs(data_path):
    chunker = ModGovMinutesChunker("EDE", data_path=data_path)
    path = chunker.write(chunker.get_chunks())
    first = path.read_bytes()
    chunker.write(chunker.get_chunks())
    assert path.read_bytes() == first
    assert path == data_path / "Embeddings" / "minutes.jsonl"


def test_written_output_reads_back(data_path, chunks):
    chunker = ModGovMinutesChunker("EDE", data_path=data_path)
    path = chunker.write(chunks)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [Chunk.from_dict(json.loads(line)) for line in lines] == sorted(
        chunks, key=lambda c: (c.parent_id, c.chunk_index)
    )


def test_has_data(data_path, tmp_path):
    assert ModGovMinutesChunker("EDE", data_path=data_path).has_data()
    assert not ModGovMinutesChunker("EDE", data_path=tmp_path / "empty").has_data()


def test_agenda_item_markers_are_removed(data_path):
    # Some councils' minutes keep ModernGov's escaped <AI9> markers as text.
    chunker = ModGovMinutesChunker("EDE", data_path=data_path)
    meeting, detail = next(chunker.meetings())
    item = detail.find("agendaitem")
    item.find(
        "minutesnonemptyhtmlbody"
    ).string = "<p><span>&lt;/AI5&gt;</span></p><p>&lt;AI6&gt;None.</p>"
    assert [c.text for c in chunker.chunks_for_item(meeting, item)] == ["None."]
