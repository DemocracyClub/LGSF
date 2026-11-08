"""
The files the pipeline writes beside each document, and the rules for
which stages a file needs.

For ``STO/documents/<sha256>.pdf``:

    <sha256>.pdf.docling-v2.json   the DoclingDocument
    <sha256>.pdf.md                its text, as Markdown
    <sha256>.pdf.chunks.jsonl      one Chunk a line
    <sha256>.pdf.titan-v2.jsonl    one EmbeddingRecord a line, named
                                   after the embedder
    <sha256>.pdf.status.json       what has been done

status.json is written last, and records each stage's result. A stage
with no entry in status.json hasn't run, and runs. That's how a run
that's stopped part way through is picked up: there's no log, as S3
can't append to one, and many workers write at once.

The JSON Schemas in schemas/v1/ describe the status, chunks and
embeddings files.
"""

import json
from dataclasses import asdict, dataclass, field, fields
from enum import StrEnum

from lgsf.embeddings.extract import (
    DOCLING_MAJOR_VERSION,
    EXTRACTED,
    FAILURES,
    ExtractStatus,
)
from lgsf.embeddings.formats import DocumentFormat
from lgsf.embeddings import records

#: The version of the status file's layout. Fields can be added without
#: changing it; a change to what a field means needs a new one.
SCHEMA_VERSION = 1


class Stage(StrEnum):
    EXTRACT = "extract"
    CHUNK = "chunk"
    EMBED = "embed"


class Outcome(StrEnum):
    """What a status says became of the file, in one word."""

    OK = "ok"
    RESTRICTED = "restricted"
    EMPTY = "empty"
    NOT_A_DOCUMENT = "not_a_document"
    FAILED = "failed"


#: The outcome each extraction status gives.
OUTCOMES = {
    ExtractStatus.OK: Outcome.OK,
    ExtractStatus.PARTIAL: Outcome.OK,
    ExtractStatus.FAILED: Outcome.FAILED,
    ExtractStatus.TIMEOUT: Outcome.FAILED,
    ExtractStatus.RESTRICTED: Outcome.RESTRICTED,
    ExtractStatus.EMPTY: Outcome.EMPTY,
    ExtractStatus.NOT_DOCUMENT: Outcome.NOT_A_DOCUMENT,
    ExtractStatus.UNSUPPORTED: Outcome.NOT_A_DOCUMENT,
}


class Suffix(StrEnum):
    """The suffix each result file adds to its document's key."""

    STATUS = "status.json"
    DOCLING = f"docling-v{DOCLING_MAJOR_VERSION}.json"
    TEXT = "md"
    CHUNKS = "chunks.jsonl"


def result_key(document_key: str, suffix: str) -> str:
    return f"{document_key}.{suffix}"


def embeddings_suffix(embedder_name: str) -> str:
    """The suffix of an embedder's file, such as ``titan-v2.jsonl``."""
    return f"{embedder_name}.jsonl"


@dataclass
class ExtractEntry:
    status: ExtractStatus
    #: Docling's major version, as in the ``.docling-v2.json`` name.
    version: int = DOCLING_MAJOR_VERSION
    #: The exact Docling version. None if Docling wasn't loaded.
    tool_version: str | None = None
    #: ISO date and time, in UTC.
    finished_at: str | None = None
    pages: int | None = None
    seconds: float | None = None
    #: "cpu" or "cuda". None if Docling wasn't loaded.
    device: str | None = None


@dataclass
class ChunkEntry:
    #: chunking.inputs_hash() of what the chunks were made from. None if
    #: chunking failed.
    inputs_hash: str | None
    version: int = field(default_factory=lambda: records.CHUNKER_VERSION)
    finished_at: str | None = None
    chunks: int = 0
    #: How many chunks have embed=True.
    embeddable: int = 0
    error: str | None = None


@dataclass
class EmbedEntry:
    model: str
    #: The chunk inputs_hash of the chunks that were embedded.
    chunk_inputs_hash: str | None
    version: int = 1
    finished_at: str | None = None
    #: How many chunks were sent to the model this time.
    embedded: int = 0
    #: How many chunks' embeddings were kept from before.
    reused: int = 0
    #: The tokens the model read this time.
    tokens: int = 0
    error: str | None = None


@dataclass
class FileStatus:
    council: str
    storage_key: str
    #: ``sha256:<hex>``, as records have it.
    content_hash: str
    format: DocumentFormat | None = None
    outcome: Outcome | None = None
    #: What went wrong, when the outcome is failed, or what Docling
    #: reported about a partial conversion.
    error: str | None = None
    extract: ExtractEntry | None = None
    chunk: ChunkEntry | None = None
    #: By embedder name.
    embed: dict[str, EmbedEntry] = field(default_factory=dict)

    @property
    def extracted(self) -> bool:
        """Whether there's a converted document to chunk."""
        return self.extract is not None and self.extract.status in EXTRACTED

    @property
    def chunked(self) -> bool:
        return (
            self.extracted
            and self.chunk is not None
            and self.chunk.error is None
            and self.chunk.inputs_hash is not None
        )

    def embedded(self, embedder_name: str) -> bool:
        entry = self.embed.get(embedder_name)
        return (
            self.chunked
            and entry is not None
            and entry.error is None
            and entry.chunk_inputs_hash == self.chunk.inputs_hash
        )

    def result_suffixes(self) -> list[str]:
        """
        The suffixes of the files this status says are complete, apart
        from status.json itself.
        """
        suffixes = []
        if self.extracted:
            suffixes += [Suffix.DOCLING, Suffix.TEXT]
        if self.chunked:
            suffixes.append(Suffix.CHUNKS)
            suffixes += [
                embeddings_suffix(name)
                for name in sorted(self.embed)
                if self.embedded(name)
            ]
        return [str(suffix) for suffix in suffixes]

    def as_dict(self) -> dict:
        return {
            "schema_version": SCHEMA_VERSION,
            "council": self.council,
            "storage_key": self.storage_key,
            "content_hash": self.content_hash,
            "format": str(self.format) if self.format else None,
            "outcome": str(self.outcome) if self.outcome else None,
            "error": self.error,
            "stages": {
                **({"extract": asdict(self.extract)} if self.extract else {}),
                **({"chunk": asdict(self.chunk)} if self.chunk else {}),
                **(
                    {
                        "embed": {
                            name: asdict(entry) for name, entry in self.embed.items()
                        }
                    }
                    if self.embed
                    else {}
                ),
            },
        }

    def as_json(self) -> bytes:
        return json.dumps(
            self.as_dict(), indent=2, ensure_ascii=False, sort_keys=True
        ).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> "FileStatus":
        """
        A status from its JSON. Keys the dataclasses don't know are
        dropped, so a field added to the files later still reads here.
        """
        stages = data.get("stages") or {}
        extract = chunk = None
        if stages.get("extract"):
            entry = dict(stages["extract"])
            entry["status"] = ExtractStatus(entry["status"])
            extract = _entry(ExtractEntry, entry)
        if stages.get("chunk"):
            chunk = _entry(ChunkEntry, stages["chunk"])
        return cls(
            council=data["council"],
            storage_key=data["storage_key"],
            content_hash=data["content_hash"],
            format=DocumentFormat(data["format"]) if data.get("format") else None,
            outcome=Outcome(data["outcome"]) if data.get("outcome") else None,
            error=data.get("error"),
            extract=extract,
            chunk=chunk,
            embed={
                name: _entry(EmbedEntry, entry)
                for name, entry in (stages.get("embed") or {}).items()
            },
        )

    @classmethod
    def from_json(cls, data: bytes) -> "FileStatus":
        return cls.from_dict(json.loads(data))


def _entry(entry_class, data: dict):
    known = {f.name for f in fields(entry_class)}
    return entry_class(**{key: value for key, value in data.items() if key in known})


#: The error of a status file that couldn't be read.
UNREADABLE = "unreadable status"


def unreadable_status(
    council: str, storage_key: str, content_hash: str
) -> "FileStatus":
    """
    What a status file that can't be parsed counts as: a failure, so the
    file isn't extracted again unless asked to with --retry-failed. The
    document's key is its content hash, so whatever the file said, it was
    about these bytes.
    """
    return FileStatus(
        council=council,
        storage_key=storage_key,
        content_hash=content_hash,
        outcome=Outcome.FAILED,
        error=UNREADABLE,
        extract=ExtractEntry(status=ExtractStatus.FAILED),
    )


def read_status(store, document_key: str, content_hash: str) -> FileStatus | None:
    """
    A document's status, or None if it has none. One for another
    content_hash counts as none: it's about other bytes. One that can't
    be parsed counts as a failure (see unreadable_status()), not as a
    free pass to run Docling again.
    """
    try:
        data = store.read_bytes(result_key(document_key, Suffix.STATUS))
    except FileNotFoundError:
        return None
    return parse_status(data, document_key, content_hash)


def parse_status(
    data: bytes, document_key: str, content_hash: str
) -> FileStatus | None:
    council, _, storage_key = document_key.partition("/documents/")
    try:
        status = FileStatus.from_json(data)
    except (ValueError, KeyError, TypeError, AttributeError):
        return unreadable_status(council, storage_key, content_hash)
    if status.content_hash != content_hash:
        return None
    return status


def write_status(store, document_key: str, status: FileStatus):
    store.write_bytes(result_key(document_key, Suffix.STATUS), status.as_json())


# The rules for each stage


def needs_extract(status: FileStatus | None, retry_failed=False, force=False) -> bool:
    """
    A file that was extracted, partly extracted or left out isn't tried
    again unless ``force``. One that failed or timed out is only tried
    again with ``retry_failed``.
    """
    if force or status is None or status.extract is None:
        return True
    if status.extract.status in FAILURES:
        return retry_failed
    return False


def needs_chunk(
    status: FileStatus | None, chunk_inputs_hash: str | None, force=False
) -> bool:
    """
    A converted document is chunked if it hasn't been, if chunking it
    failed, or if what its chunks are made from has changed.
    """
    if status is None or not status.extracted:
        return False
    if force:
        return True
    return not status.chunked or status.chunk.inputs_hash != chunk_inputs_hash


def needs_embed(status: FileStatus | None, embedder_name: str, force=False) -> bool:
    """
    Chunks are embedded by a model if they haven't been, if embedding
    them failed, or if they've changed since.
    """
    if status is None or not status.chunked:
        return False
    return force or not status.embedded(embedder_name)


@dataclass
class FilePlan:
    """What a file needs, found from its status."""

    status: FileStatus | None
    extract: bool = False
    #: Chunk the converted document saved before. A file that's
    #: extracted is always chunked afterwards, if the chunk stage is run.
    chunk: bool = False
    #: The embedders to embed with, without chunking first. A file that's
    #: chunked is always embedded afterwards, if the embed stage is run.
    embed: list[str] = field(default_factory=list)
    #: The file's size in bytes, when the plan came from a listing.
    size: int | None = None

    @property
    def needs_converter(self) -> bool:
        """Whether Docling or docling-core has to be loaded for the file."""
        return self.extract or self.chunk

    @property
    def nothing_to_do(self) -> bool:
        return not self.extract and not self.chunk and not self.embed


def plan_file(
    status: FileStatus | None,
    current_inputs_hash: str | None,
    stages,
    embedder_names,
    retry_failed=False,
    force=False,
) -> FilePlan:
    """
    Decide what a file needs from its status (already checked against
    its content_hash) by the rules above. The first stage that needs
    running is planned; the stages after it follow from it.
    """
    plan = FilePlan(status=status)
    if Stage.EXTRACT in stages and needs_extract(status, retry_failed, force):
        plan.extract = True
        return plan
    if Stage.CHUNK in stages and needs_chunk(status, current_inputs_hash, force):
        plan.chunk = True
        return plan
    if Stage.EMBED in stages:
        plan.embed = [
            name for name in embedder_names if needs_embed(status, name, force)
        ]
    return plan
