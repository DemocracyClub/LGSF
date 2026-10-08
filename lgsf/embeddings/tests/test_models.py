"""
The chunk record: what lands in JSONL, and what comes back out.

from_dict() is what the embedding step will read the JSONL with, so it
must reverse as_dict() exactly.
"""

import json

from lgsf.embeddings import Chunk, ChunkType, SourceType


def make_chunk(**kwargs):
    fields = {
        "chunk_id": "EDE:minutes:2765:18127:0",
        "parent_id": "EDE:minutes:2765:18127",
        "council": "EDE",
        "type": ChunkType.MINUTES,
        "source_type": SourceType.MODERNGOV_XML,
        "date": "2026-09-16",
        "context": "East Devon District Council · Cabinet · 16 September 2026",
        "text": "RESOLVED that Cabinet notes the report.",
        "chunk_index": 0,
        "chunk_count": 2,
        "embed": True,
        "metadata": {"minute_number": "36", "linked_attachment_ids": ["31751"]},
    }
    fields.update(kwargs)
    return Chunk(**fields)


def test_as_dict_from_dict_round_trip():
    chunk = make_chunk()
    assert Chunk.from_dict(chunk.as_dict()) == chunk


def test_round_trip_through_json():
    chunk = make_chunk()
    loaded = Chunk.from_dict(json.loads(chunk.as_json()))
    assert loaded == chunk
    assert loaded.type is ChunkType.MINUTES


def test_as_dict_writes_enums_as_strings_and_derived_fields():
    data = json.loads(make_chunk().as_json())
    assert data["type"] == "minutes"
    assert data["source_type"] == "moderngov_xml"
    assert data["word_count"] == 6
    assert len(data["text_hash"]) == 64


def test_as_json_is_one_line():
    assert "\n" not in make_chunk(text="One.\n\nTwo.").as_json()


def test_embed_text_puts_context_before_text():
    chunk = make_chunk(context="Header", text="Body")
    assert chunk.embed_text == "Header\n\nBody"


def test_text_hash_changes_when_context_changes():
    assert make_chunk().text_hash != make_chunk(context="Cabinet").text_hash


def test_text_hash_ignores_fields_not_embedded():
    assert make_chunk().text_hash == make_chunk(chunk_count=5, embed=False).text_hash
