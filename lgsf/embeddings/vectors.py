"""
Embedding files, and deciding which chunks need embedding.

Each chunk file has one embedding file per model, in a directory named
after the model:

    data/<COUNCIL>/Embeddings/minutes.jsonl            chunks
    data/<COUNCIL>/Embeddings/titan-v2/minutes.jsonl   their embeddings

so `Embeddings/*.jsonl` matches only chunk files.
"""

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

from lgsf.embeddings.embedders import EmbedderBase
from lgsf.embeddings.models import Chunk


@dataclass
class EmbeddingRecord:
    """One chunk's embedding: one line of an embedding file."""

    chunk_id: str
    #: The chunk's text_hash when it was embedded. If the chunk's hash is
    #: different now, this embedding is of old text.
    text_hash: str
    #: The provider's model ID, so each file says what made it.
    model: str
    token_count: int
    embedding: list[float]

    def as_dict(self):
        return asdict(self)

    def as_json(self):
        return json.dumps(self.as_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


def read_chunks(path: Path) -> list[Chunk]:
    with path.open(encoding="utf-8") as f:
        return [Chunk.from_dict(json.loads(line)) for line in f if line.strip()]


def embeddings_path(chunks_path: Path, embedder_name: str) -> Path:
    """Where the embeddings of the chunks in `chunks_path` are written."""
    return chunks_path.parent / embedder_name / chunks_path.name


def read_embeddings(path: Path) -> dict[str, EmbeddingRecord]:
    """Records keyed by chunk ID, or nothing if the file isn't there yet."""
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        records = (EmbeddingRecord.from_dict(json.loads(line)) for line in f)
        return {record.chunk_id: record for record in records}


def write_embeddings(path: Path, records: Sequence[EmbeddingRecord]) -> Path:
    """
    Write records as JSON lines, replacing what was there.

    Written to a temporary file that's then renamed over the old one, so a
    run that's interrupted leaves the previous file whole.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(
        "".join(f"{record.as_json()}\n" for record in records), encoding="utf-8"
    )
    temp_path.replace(path)
    return path


def embed_chunks(
    chunks: Sequence[Chunk],
    embedder: EmbedderBase,
    existing: dict[str, EmbeddingRecord],
) -> tuple[list[EmbeddingRecord], int]:
    """
    Records for every chunk to embed, and how many were newly embedded.

    An existing record is reused if it was made from the same text by the
    same model, at the same size. Everything else is sent to the embedder
    in one call. Records come back in the chunks' order, and only for
    current chunks with embed=True, so records for chunks that have gone
    are dropped.
    """
    records = {}
    to_embed = []
    for chunk in chunks:
        if not chunk.embed:
            continue
        record = existing.get(chunk.chunk_id)
        if (
            record
            and record.text_hash == chunk.text_hash
            and record.model == embedder.model
            and len(record.embedding) == embedder.dimensions
        ):
            records[chunk.chunk_id] = record
        else:
            to_embed.append(chunk)

    if to_embed:
        embeddings = embedder.embed_documents([c.embed_text for c in to_embed])
        for chunk, embedding in zip(to_embed, embeddings, strict=True):
            records[chunk.chunk_id] = EmbeddingRecord(
                chunk_id=chunk.chunk_id,
                text_hash=chunk.text_hash,
                model=embedder.model,
                token_count=embedding.token_count,
                embedding=embedding.vector,
            )

    ordered = [records[c.chunk_id] for c in chunks if c.chunk_id in records]
    return ordered, len(to_embed)
