"""
Deciding which chunks need embedding, and the files embeddings are kept in.

Uses a fake embedder, so nothing here needs a model or the network.
"""

import hashlib

import pytest

from lgsf.embeddings.embedders import Embedding, EmbedderBase
from lgsf.embeddings.models import Chunk, ChunkType, SourceType
from lgsf.embeddings.vectors import (
    EmbeddingRecord,
    embed_chunks,
    embeddings_path,
    read_chunks,
    read_embeddings,
    write_embeddings,
)


class FakeEmbedder(EmbedderBase):
    """Vectors made from each text's hash, keeping every text it's sent."""

    name = "fake"
    model = "fake-model-1"
    dimensions = 4

    def __init__(self):
        self.texts = []

    def vector(self, text):
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        return [byte / 255 for byte in digest[: self.dimensions]]

    def embed_documents(self, texts):
        self.texts.extend(texts)
        return [
            Embedding(vector=self.vector(text), token_count=len(text.split()))
            for text in texts
        ]

    def embed_query(self, text):
        return self.vector(text)


def make_chunk(index, text=None, embed=True):
    return Chunk(
        chunk_id=f"EDE:minutes:2765:18127:{index}",
        parent_id="EDE:minutes:2765:18127",
        council="EDE",
        type=ChunkType.MINUTES,
        source_type=SourceType.MODERNGOV_XML,
        date="2026-09-16",
        context="East Devon District Council · Cabinet · 16 September 2026",
        text=text or f"RESOLVED that Cabinet notes report {index}.",
        chunk_index=index,
        chunk_count=3,
        embed=embed,
    )


@pytest.fixture
def chunks():
    return [make_chunk(0), make_chunk(1, embed=False), make_chunk(2)]


def as_existing(records):
    return {record.chunk_id: record for record in records}


def test_embeds_chunks_with_embed_true_in_order(chunks):
    embedder = FakeEmbedder()
    records, embedded = embed_chunks(chunks, embedder, {})
    assert [r.chunk_id for r in records] == [
        "EDE:minutes:2765:18127:0",
        "EDE:minutes:2765:18127:2",
    ]
    assert embedded == 2
    assert embedder.texts == [chunks[0].embed_text, chunks[2].embed_text]


def test_record_fields(chunks):
    embedder = FakeEmbedder()
    record = embed_chunks(chunks, embedder, {})[0][0]
    assert record.text_hash == chunks[0].text_hash
    assert record.model == "fake-model-1"
    assert record.embedding == embedder.vector(chunks[0].embed_text)
    assert record.token_count == len(chunks[0].embed_text.split())


def test_unchanged_chunks_are_reused(chunks):
    first, _ = embed_chunks(chunks, FakeEmbedder(), {})
    embedder = FakeEmbedder()
    records, embedded = embed_chunks(chunks, embedder, as_existing(first))
    assert records == first
    assert embedded == 0
    assert embedder.texts == []


def test_changed_chunk_is_embedded_again(chunks):
    first, _ = embed_chunks(chunks, FakeEmbedder(), {})
    chunks[2] = make_chunk(2, text="RESOLVED that Cabinet rejects the report.")
    embedder = FakeEmbedder()
    records, embedded = embed_chunks(chunks, embedder, as_existing(first))
    assert embedded == 1
    assert embedder.texts == [chunks[2].embed_text]
    assert records[0] == first[0]
    assert records[1].text_hash == chunks[2].text_hash


def test_other_models_records_are_not_reused(chunks):
    first, _ = embed_chunks(chunks, FakeEmbedder(), {})

    class OtherModel(FakeEmbedder):
        model = "fake-model-2"

    records, embedded = embed_chunks(chunks, OtherModel(), as_existing(first))
    assert embedded == 2
    assert {r.model for r in records} == {"fake-model-2"}


def test_records_of_another_size_are_not_reused(chunks):
    first, _ = embed_chunks(chunks, FakeEmbedder(), {})

    class Smaller(FakeEmbedder):
        dimensions = 2

    records, embedded = embed_chunks(chunks, Smaller(), as_existing(first))
    assert embedded == 2
    assert all(len(r.embedding) == 2 for r in records)


def test_records_for_chunks_that_have_gone_are_dropped(chunks):
    first, _ = embed_chunks(chunks, FakeEmbedder(), {})
    records, _ = embed_chunks(chunks[:2], FakeEmbedder(), as_existing(first))
    assert [r.chunk_id for r in records] == ["EDE:minutes:2765:18127:0"]


def test_record_round_trip(chunks):
    record = embed_chunks(chunks, FakeEmbedder(), {})[0][0]
    assert EmbeddingRecord.from_dict(record.as_dict()) == record


def test_embeddings_path_is_in_a_directory_per_model(tmp_path):
    chunks_path = tmp_path / "EDE" / "Embeddings" / "minutes.jsonl"
    assert embeddings_path(chunks_path, "titan-v2") == (
        tmp_path / "EDE" / "Embeddings" / "titan-v2" / "minutes.jsonl"
    )


def test_read_embeddings_without_a_file_is_empty(tmp_path):
    assert read_embeddings(tmp_path / "fake" / "minutes.jsonl") == {}


def test_running_twice_writes_the_same_file_without_embedding(tmp_path, chunks):
    chunks_path = tmp_path / "minutes.jsonl"
    chunks_path.write_text("".join(f"{c.as_json()}\n" for c in chunks))
    path = embeddings_path(chunks_path, FakeEmbedder.name)

    records, _ = embed_chunks(read_chunks(chunks_path), FakeEmbedder(), {})
    write_embeddings(path, records)
    first_run = path.read_bytes()

    embedder = FakeEmbedder()
    records, _ = embed_chunks(read_chunks(chunks_path), embedder, read_embeddings(path))
    write_embeddings(path, records)

    assert embedder.texts == []
    assert path.read_bytes() == first_run
    assert [p.name for p in path.parent.iterdir()] == ["minutes.jsonl"]
