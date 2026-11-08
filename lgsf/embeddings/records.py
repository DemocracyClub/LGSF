"""
The records written to a document's chunks and embeddings files, and
deciding which chunks need embedding.
"""

import hashlib
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field

from lgsf.embeddings.embedders import EmbedderBase, Failed

#: Change this when a change to the code in chunking.py or documents.py,
#: or to the docling-core version, changes the chunks that are made.
#: Every document is then chunked again, and every chunk whose text
#: changed is embedded again. Extraction isn't repeated.
CHUNKER_VERSION = 1


@dataclass
class Chunk:
    """
    One piece of text to embed, and what's needed to find it again. One
    line of a ``.chunks.jsonl`` file.
    """

    #: Stable between runs: ``{council}:documents:{sha256 hex}:{index}``.
    chunk_id: str
    council: str
    #: ``sha256:<hex>`` of the file, as records have it.
    content_hash: str
    storage_key: str
    chunk_index: int
    chunk_count: int
    #: The document's title and the section's headings: see
    #: chunking.header(). Put in front of the text when embedding, so a
    #: chunk can be found by what it's part of. Empty when there's
    #: neither.
    header: str
    text: str
    #: The headings of the section the text is in, outermost first.
    headings: list[str] = field(default_factory=list)
    #: The pages the text is on, in order.
    pages: list[int] = field(default_factory=list)
    #: False for chunks not worth creating embeddings for. The chunker
    #: decides; the embedding step only reads this.
    embed: bool = True

    @property
    def embed_text(self) -> str:
        """The header, then the text, or the text alone."""
        if self.header:
            return f"{self.header}\n\n{self.text}"
        return self.text

    @property
    def text_hash(self) -> str:
        """
        Hash of the text that gets embedded, header included, so a change
        to either means the chunk needs embedding again.
        """
        return hashlib.sha256(self.embed_text.encode("utf-8")).hexdigest()

    @property
    def word_count(self) -> int:
        return len(self.text.split())

    def as_dict(self) -> dict:
        out = asdict(self)
        # Derived, but written out so a change can be spotted without
        # recomputing from the text.
        out["chunker_version"] = CHUNKER_VERSION
        out["text_hash"] = self.text_hash
        out["word_count"] = self.word_count
        return out

    def as_json(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict) -> "Chunk":
        data = dict(data)
        for derived in ("chunker_version", "text_hash", "word_count"):
            data.pop(derived, None)
        return cls(**data)


@dataclass
class EmbeddingRecord:
    """One chunk's embedding: one line of an embeddings file."""

    chunk_id: str
    #: The chunk's text_hash when it was embedded. If the chunk's hash is
    #: different now, this embedding is of old text.
    text_hash: str
    #: The provider's model ID, so each file says what made it.
    model: str
    dimensions: int
    #: How many tokens the model read, by its own tokenizer.
    token_count: int
    embedding: list[float]

    def as_dict(self) -> dict:
        return asdict(self)

    def as_json(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, data: dict) -> "EmbeddingRecord":
        return cls(**data)


def read_chunks(data: bytes) -> list[Chunk]:
    return [
        Chunk.from_dict(json.loads(line))
        for line in data.decode("utf-8").splitlines()
        if line.strip()
    ]


def chunks_jsonl(chunks: Sequence[Chunk]) -> bytes:
    return "".join(f"{chunk.as_json()}\n" for chunk in chunks).encode("utf-8")


def read_embeddings(data: bytes) -> dict[str, EmbeddingRecord]:
    """The records in an embeddings file, by chunk_id."""
    records = [
        EmbeddingRecord.from_dict(json.loads(line))
        for line in data.decode("utf-8").splitlines()
        if line.strip()
    ]
    return {record.chunk_id: record for record in records}


def embeddings_jsonl(records: Sequence[EmbeddingRecord]) -> bytes:
    return "".join(f"{record.as_json()}\n" for record in records).encode("utf-8")


@dataclass
class Embedded:
    """What embed_chunks() made of a document's chunks."""

    #: In the chunks' order: reused and new, for the chunks that have an
    #: embedding now.
    records: list[EmbeddingRecord]
    #: How many were newly embedded.
    embedded: int
    #: The error for each chunk the model didn't embed.
    failures: list[str]

    @property
    def reused(self) -> int:
        return len(self.records) - self.embedded


def embed_chunks(
    chunks: Sequence[Chunk],
    embedder: EmbedderBase,
    existing: dict[str, EmbeddingRecord],
) -> Embedded:
    """
    Records for every chunk to embed, and how many were newly embedded.

    An existing record is reused if it was made from the same text by the
    same model, at the same size. Everything else is sent to the embedder
    in one call. Records come back in the chunks' order, and only for
    current chunks with embed=True, so records for chunks that have gone
    are dropped. A chunk the model didn't embed has no record, and its
    error is in ``failures``; the next run sends it again.
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
            and record.dimensions == embedder.dimensions
            and len(record.embedding) == embedder.dimensions
        ):
            records[chunk.chunk_id] = record
        else:
            to_embed.append(chunk)

    failures = []
    if to_embed:
        embeddings = embedder.embed_documents([c.embed_text for c in to_embed])
        for chunk, embedding in zip(to_embed, embeddings, strict=True):
            if isinstance(embedding, Failed):
                failures.append(embedding.error)
                continue
            records[chunk.chunk_id] = EmbeddingRecord(
                chunk_id=chunk.chunk_id,
                text_hash=chunk.text_hash,
                model=embedder.model,
                dimensions=embedder.dimensions,
                token_count=embedding.token_count,
                embedding=embedding.vector,
            )

    ordered = [records[c.chunk_id] for c in chunks if c.chunk_id in records]
    return Embedded(ordered, len(to_embed) - len(failures), failures)
