import hashlib
import json
from dataclasses import asdict, dataclass, field
from enum import StrEnum


class ChunkType(StrEnum):
    """The kind of council record a chunk's text comes from."""

    MINUTES = "minutes"
    DECISIONS = "decisions"


class SourceType(StrEnum):
    """The format the chunk's text was read from."""

    #: Agenda item minutes embedded as HTML in ModernGov's GetMeeting XML.
    MODERNGOV_XML = "moderngov_xml"
    #: The JSON the decisions scraper writes for each decision. Every
    #: decisions scraper writes the same fields, whatever the CMS.
    DECISION_JSON = "decision_json"


@dataclass
class Chunk:
    """
    One piece of text to embed, and what's needed to find and filter it.

    Every chunker writes this same record, whatever its source. A field is
    at the top level if every chunk has it and it's used to identify,
    filter or embed the chunk. Anything only some sources have, or that's
    only for display, goes in `metadata`.
    """

    #: Stable between runs: `{parent_id}:{chunk_index}`.
    chunk_id: str
    #: The source this chunk was cut from, as `{council}:{type}:{...}`.
    #: A field rather than just a prefix of chunk_id because vector stores
    #: filter on fields: it's how all of a source's chunks are found, and
    #: deleted when the source shrinks or disappears.
    parent_id: str
    council: str
    type: ChunkType
    source_type: SourceType
    #: ISO date
    date: str | None
    #: A header naming where the text comes from, put in front of it when
    #: embedding so the chunk can be found by council, committee or date.
    context: str
    text: str
    chunk_index: int = 0
    chunk_count: int = 1
    #: False for chunks not worth creating embeddings for. The chunker
    #: knows its source, so it decides; the embedding step only reads this.
    embed: bool = True
    metadata: dict = field(default_factory=dict)

    @property
    def embed_text(self):
        # A chunk can be the header alone, for a source with nothing more
        # than its title.
        return "\n\n".join(part for part in (self.context, self.text) if part)

    @property
    def text_hash(self):
        """
        Hash of the text that gets embedded, header included, so a change
        to either means the chunk needs embedding again.
        """
        return hashlib.sha256(self.embed_text.encode("utf-8")).hexdigest()

    @property
    def word_count(self):
        return len(self.text.split())

    def as_dict(self):
        out = asdict(self)
        out["type"] = str(self.type)
        out["source_type"] = str(self.source_type)
        # Derived, but written out so a change can be spotted without
        # recomputing from the text.
        out["text_hash"] = self.text_hash
        out["word_count"] = self.word_count
        return out

    def as_json(self):
        return json.dumps(self.as_dict(), sort_keys=True, ensure_ascii=False)

    @classmethod
    def from_dict(cls, data):
        data = dict(data)
        data.pop("text_hash", None)
        data.pop("word_count", None)
        data["type"] = ChunkType(data["type"])
        data["source_type"] = SourceType(data["source_type"])
        return cls(**data)
