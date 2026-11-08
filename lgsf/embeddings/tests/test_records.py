"""
The chunk and embedding records, and deciding which chunks need
embedding.

from_dict() is what the embedding step reads the JSONL with, so it must
reverse as_dict() exactly. A fake embedder is used, so nothing here
needs a model or the network.
"""

import json

import pytest

from lgsf.embeddings.embedders import Embedding, FakeEmbedder, Failed
from lgsf.embeddings.records import (
    Chunk,
    EmbeddingRecord,
    chunks_jsonl,
    embed_chunks,
    embeddings_jsonl,
    read_chunks,
    read_embeddings,
)

DIGEST = "98e2" + "0" * 60


def make_chunk(index=0, **kwargs):
    fields = {
        "chunk_id": f"MAN:documents:{DIGEST}:{index}",
        "council": "MAN",
        "content_hash": f"sha256:{DIGEST}",
        "storage_key": f"{DIGEST}.pdf",
        "chunk_index": index,
        "chunk_count": 3,
        "header": "Decision notice\nNotice of Decision Made",
        "text": f"The City Treasurer approved report {index}.",
        "headings": ["Notice of Decision Made"],
        "pages": [1],
        "embed": True,
    }
    fields.update(kwargs)
    return Chunk(**fields)


@pytest.fixture
def chunks():
    return [make_chunk(0), make_chunk(1, embed=False), make_chunk(2)]


def as_existing(records):
    return {record.chunk_id: record for record in records}


# Chunk


def test_as_dict_from_dict_round_trip():
    chunk = make_chunk()
    assert Chunk.from_dict(chunk.as_dict()) == chunk


def test_round_trip_through_jsonl(chunks):
    assert read_chunks(chunks_jsonl(chunks)) == chunks


def test_as_dict_writes_derived_fields():
    data = json.loads(make_chunk().as_json())
    assert data["chunker_version"] == 1
    assert data["word_count"] == 6
    assert len(data["text_hash"]) == 64


def test_as_json_is_one_sorted_line():
    text = make_chunk(text="One.\n\nTwo.").as_json()
    assert "\n" not in text
    keys = list(json.loads(text))
    assert keys == sorted(keys)


def test_as_json_keeps_non_ascii():
    assert "£1,395" in make_chunk(text="Grant of £1,395 agreed.").as_json()


def test_embed_text_puts_header_before_text():
    assert make_chunk(header="Header", text="Body").embed_text == "Header\n\nBody"


def test_embed_text_without_a_header_is_the_text():
    assert make_chunk(header="", text="Body").embed_text == "Body"


def test_text_hash_changes_when_header_changes():
    assert make_chunk().text_hash != make_chunk(header="Other").text_hash


def test_text_hash_ignores_fields_not_embedded():
    assert make_chunk().text_hash == make_chunk(chunk_count=5, embed=False).text_hash


# embed_chunks()


def test_embeds_chunks_with_embed_true_in_order(chunks):
    embedder = FakeEmbedder()
    result = embed_chunks(chunks, embedder, {})
    assert [r.chunk_id for r in result.records] == [
        f"MAN:documents:{DIGEST}:0",
        f"MAN:documents:{DIGEST}:2",
    ]
    assert result.embedded == 2
    assert result.reused == 0
    assert result.failures == []
    assert embedder.texts == [chunks[0].embed_text, chunks[2].embed_text]


def test_record_fields(chunks):
    embedder = FakeEmbedder()
    record = embed_chunks(chunks, embedder, {}).records[0]
    assert record.text_hash == chunks[0].text_hash
    assert record.model == "fake-sha256"
    assert record.dimensions == 4
    assert record.embedding == embedder.vector(chunks[0].embed_text)
    assert record.token_count == len(chunks[0].embed_text.split())


def test_unchanged_chunks_are_reused(chunks):
    first = embed_chunks(chunks, FakeEmbedder(), {}).records
    embedder = FakeEmbedder()
    result = embed_chunks(chunks, embedder, as_existing(first))
    assert result.records == first
    assert result.embedded == 0
    assert result.reused == 2
    assert embedder.texts == []


def test_changed_chunk_is_embedded_again(chunks):
    first = embed_chunks(chunks, FakeEmbedder(), {}).records
    chunks[2] = make_chunk(2, text="The City Treasurer rejected the report.")
    embedder = FakeEmbedder()
    result = embed_chunks(chunks, embedder, as_existing(first))
    assert result.embedded == 1
    assert embedder.texts == [chunks[2].embed_text]
    assert result.records[0] == first[0]
    assert result.records[1].text_hash == chunks[2].text_hash


def test_other_models_records_are_not_reused(chunks):
    first = embed_chunks(chunks, FakeEmbedder(), {}).records

    class OtherModel(FakeEmbedder):
        model = "fake-2"

    result = embed_chunks(chunks, OtherModel(), as_existing(first))
    assert result.embedded == 2
    assert {r.model for r in result.records} == {"fake-2"}


def test_records_of_another_size_are_not_reused(chunks):
    first = embed_chunks(chunks, FakeEmbedder(), {}).records

    class Smaller(FakeEmbedder):
        dimensions = 2

    result = embed_chunks(chunks, Smaller(), as_existing(first))
    assert result.embedded == 2
    assert all(len(r.embedding) == 2 for r in result.records)


def test_records_for_chunks_that_have_gone_are_dropped(chunks):
    first = embed_chunks(chunks, FakeEmbedder(), {}).records
    result = embed_chunks(chunks[:2], FakeEmbedder(), as_existing(first))
    assert [r.chunk_id for r in result.records] == [f"MAN:documents:{DIGEST}:0"]


class HalfBroken(FakeEmbedder):
    """Fails on the text with "report 2" in it."""

    def embed_documents(self, texts):
        return [
            Failed("ThrottlingException: slow down")
            if "report 2" in text
            else Embedding(self.vector(text), len(text.split()))
            for text in texts
        ]


def test_a_failed_chunk_has_no_record_and_the_rest_are_kept(chunks):
    result = embed_chunks(chunks, HalfBroken(), {})
    assert [r.chunk_id for r in result.records] == [f"MAN:documents:{DIGEST}:0"]
    assert result.embedded == 1
    assert result.failures == ["ThrottlingException: slow down"]


def test_only_the_failed_chunk_is_sent_again(chunks):
    first = embed_chunks(chunks, HalfBroken(), {}).records
    embedder = FakeEmbedder()
    result = embed_chunks(chunks, embedder, as_existing(first))
    assert embedder.texts == [chunks[2].embed_text]
    assert result.embedded == 1
    assert result.reused == 1
    assert len(result.records) == 2


def test_record_round_trip(chunks):
    records = embed_chunks(chunks, FakeEmbedder(), {}).records
    assert EmbeddingRecord.from_dict(records[0].as_dict()) == records[0]
    assert read_embeddings(embeddings_jsonl(records)) == as_existing(records)
