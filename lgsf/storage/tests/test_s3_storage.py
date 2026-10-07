"""
S3 storage: the local backends with a bucket in place of data/.

Run against an in-memory fake of the S3 client, so what is checked is what
we ask S3 to do and in what order, not S3 itself.
"""

import io
import json
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from lgsf.storage import s3
from lgsf.storage.backends import get_storage_backend
from lgsf.storage.backends.base import StorageMode
from lgsf.storage.backends.s3 import S3Storage
from lgsf.storage.documents import get_document_storage_backend, hash_content
from lgsf.storage.documents.s3 import S3DocumentStorage


class FakeS3:
    """Enough of a boto3 S3 client for these backends, and a call log."""

    def __init__(self, page_size=1000):
        self.objects = {}
        self.calls = []
        self.page_size = page_size
        self.fail_on = None

    def _missing(self, op):
        return ClientError({"Error": {"Code": "NoSuchKey"}}, op)

    def put_object(self, Bucket, Key, Body, ContentType=None, Metadata=None):
        self.calls.append(("put", Key))
        if self.fail_on and self.fail_on in Key:
            raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")
        self.objects[Key] = {
            "Body": bytes(Body),
            "ContentType": ContentType,
            "Metadata": Metadata or {},
        }

    def get_object(self, Bucket, Key):
        self.calls.append(("get", Key))
        if Key not in self.objects:
            raise self._missing("GetObject")
        return {"Body": io.BytesIO(self.objects[Key]["Body"])}

    def head_object(self, Bucket, Key):
        self.calls.append(("head", Key))
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        obj = self.objects[Key]
        return {"ContentLength": len(obj["Body"]), "Metadata": obj["Metadata"]}

    def delete_objects(self, Bucket, Delete):
        for item in Delete["Objects"]:
            self.calls.append(("delete", item["Key"]))
            self.objects.pop(item["Key"], None)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                fake.calls.append(("list", Prefix))
                keys = sorted(k for k in fake.objects if k.startswith(Prefix))
                for start in range(0, len(keys), fake.page_size):
                    yield {
                        "Contents": [
                            {"Key": k} for k in keys[start : start + fake.page_size]
                        ]
                    }

        return Paginator()

    def ops(self, kind):
        return [key for op, key in self.calls if op == kind]


@pytest.fixture
def fake():
    return FakeS3()


def metadata_store(fake, mode=StorageMode.ACCUMULATE, prefix="data"):
    return S3Storage(
        council_code="KIR",
        scraper_object_type="Decisions",
        storage_mode=mode,
        bucket="bucket",
        prefix=prefix,
        client=fake,
    )


# ---- metadata ----


def test_the_layout_is_the_local_data_directory(fake):
    store = metadata_store(fake)
    with store.session("run") as session:
        session.write(Path("json/2026-01-01-1.json"), "{}")
        session.write(Path("_index.json"), "{}")

    assert set(fake.objects) == {
        "data/KIR/Decisions/json/2026-01-01-1.json",
        "data/KIR/Decisions/_index.json",
    }


def test_nothing_is_uploaded_until_the_session_ends(fake):
    store = metadata_store(fake)
    session = store.start_session()
    session.write(Path("json/a.json"), "{}")

    assert fake.objects == {}
    assert session.open(Path("json/a.json")) == "{}"

    store.end_session(session, "checkpoint")
    assert "data/KIR/Decisions/json/a.json" in fake.objects


def test_the_index_goes_up_after_the_records_it_names(fake):
    """Someone syncing mid-run never sees an index naming missing files."""
    store = metadata_store(fake)
    with store.session("run") as session:
        session.write(Path("_index.json"), "{}")
        for i in range(20):
            session.write(Path(f"json/{i}.json"), "{}")

    assert fake.ops("put")[-1] == "data/KIR/Decisions/_index.json"


def test_a_failed_upload_leaves_the_index_as_it_was(fake):
    store = metadata_store(fake)
    fake.fail_on = "json/3.json"
    session = store.start_session()
    session.write(Path("_index.json"), '{"new": true}')
    for i in range(5):
        session.write(Path(f"json/{i}.json"), "{}")

    with pytest.raises(ClientError):
        store.end_session(session, "checkpoint")

    assert "data/KIR/Decisions/_index.json" not in fake.objects


def test_files_open_properly_from_the_console(fake):
    store = metadata_store(fake)
    with store.session("run") as session:
        session.write(Path("json/a.json"), "{}")
        session.write(Path("raw/a.html"), "<p>")

    assert fake.objects["data/KIR/Decisions/json/a.json"]["ContentType"] == (
        "application/json; charset=utf-8"
    )
    assert fake.objects["data/KIR/Decisions/raw/a.html"]["ContentType"] == (
        "text/html; charset=utf-8"
    )


def test_reads_fall_back_to_s3_and_missing_files_are_not_found(fake):
    fake.objects["data/KIR/Decisions/_index.json"] = {"Body": b'{"a": 1}'}
    store = metadata_store(fake)
    session = store.start_session()

    assert json.loads(session.open(Path("_index.json"))) == {"a": 1}
    with pytest.raises(FileNotFoundError):
        session.open(Path("json/missing.json"))


def test_existence_is_answered_from_one_listing(fake):
    """A request per record would be thousands per council per run."""
    fake.page_size = 2
    for i in range(5):
        fake.objects[f"data/KIR/Decisions/json/{i}.json"] = {"Body": b"{}"}
    store = metadata_store(fake)

    session = store.start_session()
    assert all(session.exists(Path(f"json/{i}.json")) for i in range(5))
    assert not session.exists(Path("json/9.json"))
    session.write(Path("json/9.json"), "{}")
    assert session.exists(Path("json/9.json"))
    store.end_session(session, "checkpoint")

    # Later sessions on the same backend - checkpoints - reuse the listing,
    # which now includes what was uploaded.
    for _ in range(3):
        session = store.start_session()
        assert session.exists(Path("json/9.json"))
        assert all(session.exists(Path(f"json/{i}.json")) for i in range(5))
        store.end_session(session, "checkpoint")

    assert fake.ops("list") == ["data/KIR/Decisions/"]
    assert fake.ops("get") == [] and fake.ops("head") == []


def test_other_councils_and_types_are_not_listed(fake):
    fake.objects["data/KIRX/Decisions/json/a.json"] = {"Body": b"{}"}
    fake.objects["data/KIR/Councillors/json/a.json"] = {"Body": b"{}"}
    session = metadata_store(fake).start_session()

    assert not session.exists(Path("json/a.json"))


def test_accumulate_keeps_what_earlier_runs_wrote(fake):
    fake.objects["data/KIR/Decisions/json/old.json"] = {"Body": b"{}"}
    store = metadata_store(fake)
    with store.session("run") as session:
        session.write(Path("json/new.json"), "{}")

    assert "data/KIR/Decisions/json/old.json" in fake.objects


def test_replace_removes_what_this_run_did_not_write_once_it_is_up(fake):
    fake.objects["data/KIR/Decisions/json/departed.json"] = {"Body": b"{}"}
    store = metadata_store(fake, mode=StorageMode.REPLACE)
    with store.session("run") as session:
        session.write(Path("json/current.json"), "{}")

    assert set(fake.objects) == {"data/KIR/Decisions/json/current.json"}
    calls = [op for op, _ in fake.calls if op in ("put", "delete")]
    assert calls == ["put", "delete"]


def test_replace_with_a_failed_run_leaves_the_old_data(fake):
    fake.objects["data/KIR/Decisions/json/departed.json"] = {"Body": b"{}"}
    store = metadata_store(fake, mode=StorageMode.REPLACE)
    session = store.start_session()
    session.write(Path("json/current.json"), "{}")
    store._reset_session_state(session)

    assert "data/KIR/Decisions/json/departed.json" in fake.objects


def test_paths_out_of_the_council_are_refused(fake):
    session = metadata_store(fake).start_session()
    for bad in ("../KIRX/json/a.json", "/etc/passwd"):
        with pytest.raises(ValueError):
            session.write(Path(bad), "{}")


def test_no_prefix_puts_councils_at_the_top_of_the_bucket(fake):
    store = metadata_store(fake, prefix="")
    with store.session("run") as session:
        session.write(Path("json/a.json"), "{}")

    assert set(fake.objects) == {"KIR/Decisions/json/a.json"}


def test_run_logs_can_be_written_outside_a_session(fake):
    """A failed run's session never commits, and its log matters most."""
    metadata_store(fake).write_now("runlog.json", "{}")

    assert "data/KIR/Decisions/runlog.json" in fake.objects


def test_a_bucket_is_required(monkeypatch, fake):
    monkeypatch.delenv("LGSF_S3_BUCKET", raising=False)
    with pytest.raises(ValueError, match="LGSF_S3_BUCKET"):
        S3Storage(council_code="KIR", client=fake)


# ---- documents ----


def document_store(fake):
    return S3DocumentStorage("KIR", bucket="bucket", prefix="data", client=fake)


def test_documents_sit_beside_the_metadata(fake):
    stored = document_store(fake).write("2026-01-01-1-s123.pdf", b"%PDF")

    obj = fake.objects["data/KIR/documents/2026-01-01-1-s123.pdf"]
    assert obj["ContentType"] == "application/pdf"
    assert stored.url == "s3://bucket/data/KIR/documents/2026-01-01-1-s123.pdf"
    assert stored.backend == "s3"


def test_describing_a_document_does_not_download_it(fake):
    """Recovering after a crash would otherwise re-download every file."""
    store = document_store(fake)
    written = store.write("a.pdf", b"%PDF-1.7 report")
    fresh = document_store(fake)

    described = fresh.describe("a.pdf")

    assert described.content_hash == written.content_hash
    assert described.content_length == written.content_length
    assert fake.ops("get") == []


def test_a_document_put_there_some_other_way_is_hashed_the_slow_way(fake):
    fake.objects["data/KIR/documents/a.pdf"] = {"Body": b"%PDF", "Metadata": {}}

    described = document_store(fake).describe("a.pdf")

    assert described.content_hash == hash_content(b"%PDF")


def test_document_existence_is_answered_from_one_listing(fake):
    fake.objects["data/KIR/documents/a.pdf"] = {"Body": b"%PDF"}
    store = document_store(fake)

    assert store.exists("a.pdf")
    assert not store.exists("b.pdf")
    store.write("b.pdf", b"x")
    assert store.exists("b.pdf")

    assert fake.ops("list") == ["data/KIR/documents/"]
    assert fake.ops("head") == []


def test_a_missing_document_is_not_found(fake):
    with pytest.raises(FileNotFoundError):
        document_store(fake).read("missing.pdf")
    assert document_store(fake).describe("missing.pdf") is None


# ---- choosing it ----


def test_s3_is_chosen_from_the_environment(monkeypatch, fake):
    monkeypatch.setattr(s3, "get_client", lambda: fake)
    monkeypatch.setenv("LGSF_STORAGE_BACKEND", "s3")
    monkeypatch.setenv("LGSF_S3_BUCKET", "bucket")
    monkeypatch.setenv("LGSF_S3_PREFIX", "/runs/2026/")
    monkeypatch.delenv("LGSF_DOCUMENT_STORAGE_BACKEND", raising=False)

    store = get_storage_backend(
        council_code="KIR",
        options={"council": "KIR"},
        scraper_object_type="Decisions",
        storage_mode=StorageMode.ACCUMULATE,
    )
    documents = get_document_storage_backend("KIR", options={"council": "KIR"})

    assert isinstance(store, S3Storage)
    assert store.root == "runs/2026/KIR/Decisions"
    assert store.supports_checkpoints
    # Documents follow the metadata to S3 unless told otherwise: hundreds
    # of gigabytes of PDFs on the local disk is never what was meant.
    assert isinstance(documents, S3DocumentStorage)
    assert documents.root == "runs/2026/KIR/documents"


def test_documents_can_still_be_sent_elsewhere(monkeypatch, fake):
    monkeypatch.setattr(s3, "get_client", lambda: fake)
    monkeypatch.setenv("LGSF_STORAGE_BACKEND", "s3")
    monkeypatch.setenv("LGSF_DOCUMENT_STORAGE_BACKEND", "local")

    from lgsf.storage.documents.local import LocalDocumentStorage

    assert isinstance(
        get_document_storage_backend("KIR", options={}), LocalDocumentStorage
    )
