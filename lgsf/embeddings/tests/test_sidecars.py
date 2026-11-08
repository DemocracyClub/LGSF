"""status.json, and the rules for which stages a file needs."""

import dataclasses
import json

import pytest

from lgsf.embeddings.chunking import inputs_hash
from lgsf.embeddings.extract import ExtractStatus
from lgsf.embeddings.files import File
from lgsf.embeddings.formats import DocumentFormat
from lgsf.embeddings.sidecars import (
    ChunkEntry,
    EmbedEntry,
    ExtractEntry,
    UNREADABLE,
    FileStatus,
    Outcome,
    Stage,
    Suffix,
    needs_chunk,
    needs_embed,
    needs_extract,
    plan_file,
    read_status,
    result_key,
    write_status,
)
from lgsf.embeddings.stores import LocalStore

ALL_STAGES = list(Stage)
EMBEDDERS = ["titan-v2"]
DIGEST = "a" * 64
FILE = File("STO", f"{DIGEST}.pdf")
EXTRACT_ENTRY = ExtractEntry(
    status=ExtractStatus.OK,
    tool_version="2.130.0",
    finished_at="2026-10-19T12:00:00+00:00",
    pages=2,
    seconds=3.1,
    device="cpu",
)


def chunk_hash(status):
    return inputs_hash(status.format, status.extract.tool_version, status.content_hash)


def done_status(file=FILE, **changes):
    """The status of a file that's been through every stage."""
    status = FileStatus(
        council=file.council,
        storage_key=file.storage_key,
        content_hash=file.content_hash,
        format=DocumentFormat.PDF,
        outcome=Outcome.OK,
        extract=EXTRACT_ENTRY,
    )
    status.chunk = ChunkEntry(
        inputs_hash=chunk_hash(status),
        finished_at="2026-10-19T12:00:01+00:00",
        chunks=4,
        embeddable=3,
    )
    status.embed = {
        "titan-v2": EmbedEntry(
            model="amazon.titan-embed-text-v2:0",
            chunk_inputs_hash=status.chunk.inputs_hash,
            finished_at="2026-10-19T12:00:02+00:00",
            embedded=3,
            tokens=1234,
        )
    }
    return dataclasses.replace(status, **changes)


def extracted(extract_status=ExtractStatus.OK):
    status = done_status(chunk=None, embed={})
    status.extract = dataclasses.replace(EXTRACT_ENTRY, status=extract_status)
    return status


# status.json


def test_status_round_trip():
    status = done_status()
    loaded = FileStatus.from_json(status.as_json())
    assert loaded == status
    assert loaded.extract.status is ExtractStatus.OK
    assert loaded.format is DocumentFormat.PDF
    assert loaded.outcome is Outcome.OK


def test_status_layout():
    data = json.loads(done_status().as_json())
    assert data["schema_version"] == 1
    assert data["council"] == "STO"
    assert data["storage_key"] == f"{DIGEST}.pdf"
    assert data["content_hash"] == f"sha256:{DIGEST}"
    assert data["format"] == "pdf"
    assert data["outcome"] == "ok"
    assert data["error"] is None
    assert sorted(data["stages"]) == ["chunk", "embed", "extract"]
    assert data["stages"]["extract"]["version"] == 2
    assert data["stages"]["extract"]["tool_version"] == "2.130.0"
    assert data["stages"]["chunk"]["version"] == 1
    assert (
        data["stages"]["embed"]["titan-v2"]["model"] == "amazon.titan-embed-text-v2:0"
    )


def test_stages_are_absent_until_they_run():
    status = FileStatus(
        council="STO", storage_key=f"{DIGEST}.pdf", content_hash="sha256:x"
    )
    assert json.loads(status.as_json())["stages"] == {}
    data = json.loads(extracted().as_json())
    assert list(data["stages"]) == ["extract"]


def test_status_of_a_left_out_file_round_trip():
    status = FileStatus(
        council="STO",
        storage_key=f"{DIGEST}.html",
        content_hash=f"sha256:{DIGEST}",
        format=DocumentFormat.RESTRICTED,
        outcome=Outcome.RESTRICTED,
        extract=ExtractEntry(status=ExtractStatus.RESTRICTED),
    )
    assert FileStatus.from_json(status.as_json()) == status


def test_read_and_write_status(tmp_path):
    store = LocalStore(tmp_path)
    assert read_status(store, FILE.document_key, FILE.content_hash) is None
    write_status(store, FILE.document_key, done_status())
    assert read_status(store, FILE.document_key, FILE.content_hash) == done_status()
    assert store.exists(f"{FILE.document_key}.status.json")


def test_status_for_other_bytes_is_missing(tmp_path):
    store = LocalStore(tmp_path)
    write_status(store, FILE.document_key, done_status(content_hash="sha256:old"))
    assert read_status(store, FILE.document_key, FILE.content_hash) is None


@pytest.mark.parametrize("data", [b'{"content_', b'{"format": "x"}', b"[]"])
def test_unreadable_status_is_a_failure(tmp_path, data):
    store = LocalStore(tmp_path)
    store.write_bytes(result_key(FILE.document_key, Suffix.STATUS), data)
    status = read_status(store, FILE.document_key, FILE.content_hash)
    assert status.outcome == Outcome.FAILED
    assert status.error == UNREADABLE
    assert status.extract.status == ExtractStatus.FAILED
    assert (status.council, status.storage_key) == ("STO", FILE.storage_key)
    assert status.content_hash == FILE.content_hash


def test_a_corrupt_status_does_not_extract_again(tmp_path):
    store = LocalStore(tmp_path)
    store.write_bytes(result_key(FILE.document_key, Suffix.STATUS), b"{{")
    status = read_status(store, FILE.document_key, FILE.content_hash)
    assert plan(status).nothing_to_do
    assert plan(status, retry_failed=True).extract


def test_fields_added_later_are_ignored():
    data = json.loads(done_status().as_json())
    data["stages"]["extract"]["ocr_engine"] = "rapidocr"
    data["stages"]["chunk"]["tokenizer"] = "words"
    data["stages"]["embed"]["titan-v2"]["region"] = "eu-west-2"
    data["later"] = True
    status = FileStatus.from_dict(data)
    assert status == done_status()
    assert plan(status).nothing_to_do


def test_result_suffixes():
    assert done_status().result_suffixes() == [
        "docling-v2.json",
        "md",
        "chunks.jsonl",
        "titan-v2.jsonl",
    ]
    status = done_status()
    status.embed["titan-v2"].error = "ThrottlingException"
    assert status.result_suffixes() == ["docling-v2.json", "md", "chunks.jsonl"]
    assert extracted(ExtractStatus.FAILED).result_suffixes() == []


# Extract


def test_extract_without_a_status():
    assert needs_extract(None)


@pytest.mark.parametrize(
    "extract_status",
    [
        ExtractStatus.OK,
        ExtractStatus.PARTIAL,
        ExtractStatus.RESTRICTED,
        ExtractStatus.NOT_DOCUMENT,
        ExtractStatus.EMPTY,
        ExtractStatus.UNSUPPORTED,
    ],
)
def test_extract_is_not_repeated(extract_status):
    status = extracted(extract_status)
    assert not needs_extract(status)
    assert not needs_extract(status, retry_failed=True)
    assert needs_extract(status, force=True)


@pytest.mark.parametrize(
    "extract_status", [ExtractStatus.FAILED, ExtractStatus.TIMEOUT]
)
def test_failures_are_only_retried_when_asked(extract_status):
    status = extracted(extract_status)
    assert not needs_extract(status)
    assert needs_extract(status, retry_failed=True)


# Chunk


def test_chunk_after_extracting():
    status = extracted()
    assert needs_chunk(status, chunk_hash(status))


def test_chunk_is_not_repeated():
    status = done_status()
    assert not needs_chunk(status, chunk_hash(status))
    assert needs_chunk(status, chunk_hash(status), force=True)


def test_chunk_again_when_the_inputs_change():
    assert needs_chunk(done_status(), "sha256:other")


def test_chunk_again_after_an_error():
    status = done_status()
    status.chunk = ChunkEntry(inputs_hash=None, error="Bad")
    assert needs_chunk(status, done_status().chunk.inputs_hash)


@pytest.mark.parametrize(
    "extract_status", [ExtractStatus.FAILED, ExtractStatus.RESTRICTED]
)
def test_nothing_to_chunk(extract_status):
    assert not needs_chunk(None, None)
    assert not needs_chunk(extracted(extract_status), None)


# Embed


def test_embed_after_chunking():
    assert needs_embed(done_status(embed={}), "titan-v2")


def test_embed_is_not_repeated():
    status = done_status()
    assert not needs_embed(status, "titan-v2")
    assert needs_embed(status, "titan-v2", force=True)


def test_embed_with_another_model():
    assert needs_embed(done_status(), "fake")


def test_embed_again_after_an_error():
    status = done_status()
    status.embed["titan-v2"].error = "ThrottlingException"
    assert needs_embed(status, "titan-v2")


def test_embed_again_when_the_chunks_change():
    status = done_status()
    status.chunk.inputs_hash = "sha256:other"
    assert needs_embed(status, "titan-v2")


def test_nothing_to_embed():
    assert not needs_embed(None, "titan-v2")
    assert not needs_embed(extracted(), "titan-v2")
    status = done_status()
    status.chunk.error = "Bad"
    assert not needs_embed(status, "titan-v2")


# Planning a file


def plan(status, stages=ALL_STAGES, **kwargs):
    current = chunk_hash(status) if status and status.extracted else None
    return plan_file(status, current, stages, EMBEDDERS, **kwargs)


def test_plan_a_new_file():
    result = plan(None)
    assert result.extract
    assert result.needs_converter


def test_plan_a_file_that_is_done():
    result = plan(done_status())
    assert not result.needs_converter
    assert result.embed == []
    assert result.nothing_to_do


def test_plan_a_new_chunker_version(monkeypatch):
    status = done_status()
    monkeypatch.setattr("lgsf.embeddings.records.CHUNKER_VERSION", 999)
    result = plan(status)
    assert not result.extract
    assert result.chunk


def test_plan_only_the_stages_asked_for():
    assert not plan(None, [Stage.CHUNK]).needs_converter
    status = extracted()
    result = plan(status, [Stage.EXTRACT, Stage.EMBED])
    assert not result.needs_converter
    assert result.embed == []
    assert plan(status, [Stage.CHUNK]).chunk


def test_plan_embedding_alone():
    result = plan(done_status(embed={}))
    assert not result.needs_converter
    assert result.embed == ["titan-v2"]


def test_force():
    assert plan(done_status(), force=True).extract


def test_retry_failed():
    status = extracted(ExtractStatus.TIMEOUT)
    assert plan(status).nothing_to_do
    assert plan(status, retry_failed=True).extract
