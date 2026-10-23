"""
S3 storage: the local backends with a bucket in place of data/.

Run against an in-memory fake of the S3 client, so what is checked is what
we ask S3 to do and in what order, not S3 itself.
"""

import hashlib
import io
import json
import re
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from lgsf.storage import s3
from lgsf.storage.backends import get_storage_backend
from lgsf.storage.backends.base import StorageMode
from lgsf.storage.backends.s3 import S3Storage
from lgsf.storage.documents import get_document_storage_backend
from lgsf.storage.documents.s3 import S3DocumentStorage


class FakeS3:
    """
    Enough of a boto3 S3 client for these backends and the documents
    migration, and a call log.

    Objects are dicts of Body, and optionally ContentType and Metadata, so
    a test can put one there directly.
    """

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

    def get_object(self, Bucket, Key, Range=None):
        """
        The object, or with Range="bytes=<first>-<last>" part of it. As S3
        does, a range starting past the end, which is any range of an empty
        object, fails with InvalidRange, and one ending past it is cut short.
        """
        self.calls.append(("range" if Range else "get", Key))
        if Key not in self.objects:
            raise self._missing("GetObject")
        obj = self.objects[Key]
        body = obj["Body"]
        response = {
            "ContentType": obj.get("ContentType"),
            "Metadata": dict(obj.get("Metadata") or {}),
            "ETag": etag(body),
        }
        if Range:
            first, last = map(int, re.fullmatch(r"bytes=(\d+)-(\d+)", Range).groups())
            if first >= len(body):
                raise ClientError({"Error": {"Code": "InvalidRange"}}, "GetObject")
            part = body[first : last + 1]
            response["ContentRange"] = (
                f"bytes {first}-{first + len(part) - 1}/{len(body)}"
            )
            body = part
        response["ContentLength"] = len(body)
        response["Body"] = io.BytesIO(body)
        return response

    def head_object(self, Bucket, Key):
        self.calls.append(("head", Key))
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        obj = self.objects[Key]
        return {
            "ContentLength": len(obj["Body"]),
            "Metadata": dict(obj.get("Metadata") or {}),
        }

    def copy_object(
        self,
        Bucket,
        Key,
        CopySource,
        MetadataDirective="COPY",
        Metadata=None,
        ContentType=None,
    ):
        """
        As S3 does, COPY keeps the source's metadata and content type and
        REPLACE takes the ones given.
        """
        self.calls.append(("copy", Key))
        if self.fail_on and self.fail_on in Key:
            raise ClientError({"Error": {"Code": "InternalError"}}, "CopyObject")
        source = self.objects.get(CopySource["Key"])
        if source is None:
            raise self._missing("CopyObject")
        if MetadataDirective == "REPLACE":
            metadata, content_type = Metadata or {}, ContentType
        else:
            metadata = source.get("Metadata") or {}
            content_type = source.get("ContentType")
        self.objects[Key] = {
            "Body": source["Body"],
            "ContentType": content_type,
            "Metadata": dict(metadata),
        }

    def delete_objects(self, Bucket, Delete):
        for item in Delete["Objects"]:
            self.calls.append(("delete", item["Key"]))
            self.objects.pop(item["Key"], None)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix, Delimiter=None):
                fake.calls.append(("list", Prefix))
                keys = sorted(k for k in fake.objects if k.startswith(Prefix))
                if Delimiter:
                    # Keys with the delimiter after the prefix are rolled
                    # up into their common prefix, as S3 does.
                    common = sorted(
                        {
                            Prefix + k[len(Prefix) :].split(Delimiter)[0] + Delimiter
                            for k in keys
                            if Delimiter in k[len(Prefix) :]
                        }
                    )
                    keys = [k for k in keys if Delimiter not in k[len(Prefix) :]]
                    yield {
                        "CommonPrefixes": [{"Prefix": c} for c in common],
                        "Contents": [
                            {"Key": k, "Size": len(fake.objects[k]["Body"])}
                            for k in keys
                        ],
                    }
                    return
                for start in range(0, len(keys), fake.page_size):
                    yield {
                        "Contents": [
                            {
                                "Key": k,
                                "Size": len(fake.objects[k]["Body"]),
                                "ETag": etag(fake.objects[k]["Body"]),
                            }
                            for k in keys[start : start + fake.page_size]
                        ]
                    }

        return Paginator()

    def ops(self, kind):
        return [key for op, key in self.calls if op == kind]


def etag(body):
    """What S3 gives an object put in one request: its MD5, quoted."""
    return '"{}"'.format(hashlib.md5(body).hexdigest())


@pytest.fixture
def fake():
    return FakeS3()


# ---- the fake itself ----


def test_the_fake_reads_part_of_an_object_with_its_metadata(fake):
    fake.put_object("bucket", "a.pdf", b"%PDF-1.7", Metadata={"sha256": "abc"})

    response = fake.get_object(Bucket="bucket", Key="a.pdf", Range="bytes=0-3")

    assert response["Body"].read() == b"%PDF"
    assert response["ContentRange"] == "bytes 0-3/8"
    assert response["Metadata"] == {"sha256": "abc"}
    full = fake.get_object(Bucket="bucket", Key="a.pdf", Range="bytes=0-8191")
    assert full["Body"].read() == b"%PDF-1.7"


def test_the_fake_refuses_a_range_of_an_empty_object_as_s3_does(fake):
    fake.put_object("bucket", "empty", b"")

    with pytest.raises(ClientError) as error:
        fake.get_object(Bucket="bucket", Key="empty", Range="bytes=0-8191")
    assert error.value.response["Error"]["Code"] == "InvalidRange"


def test_the_fake_copies_or_replaces_metadata(fake):
    fake.put_object(
        "bucket", "a", b"x", ContentType="application/pdf", Metadata={"sha256": "1"}
    )

    fake.copy_object(
        Bucket="bucket", Key="b", CopySource={"Bucket": "bucket", "Key": "a"}
    )
    fake.copy_object(
        Bucket="bucket",
        Key="c",
        CopySource={"Bucket": "bucket", "Key": "a"},
        MetadataDirective="REPLACE",
        Metadata={"sha256": "2"},
        ContentType="text/html",
    )

    assert fake.objects["b"]["Metadata"] == {"sha256": "1"}
    assert fake.objects["b"]["ContentType"] == "application/pdf"
    assert fake.objects["c"]["Metadata"] == {"sha256": "2"}
    assert fake.objects["c"]["ContentType"] == "text/html"
    assert fake.objects["c"]["Body"] == b"x"


def test_the_fake_lists_sizes_and_etags(fake):
    fake.put_object("bucket", "data/a", b"abc")

    (page,) = fake.get_paginator("list_objects_v2").paginate(
        Bucket="bucket", Prefix="data/"
    )

    assert page["Contents"] == [
        {"Key": "data/a", "Size": 3, "ETag": '"900150983cd24fb0d6963f7d28e17f72"'}
    ]


def test_the_fake_lists_common_prefixes(fake):
    for key in ("data/ABC/Decisions/_index.json", "data/XYZ/documents/a", "data/b"):
        fake.put_object("bucket", key, b"")

    (page,) = fake.get_paginator("list_objects_v2").paginate(
        Bucket="bucket", Prefix="data/", Delimiter="/"
    )

    assert page["CommonPrefixes"] == [{"Prefix": "data/ABC/"}, {"Prefix": "data/XYZ/"}]
    assert [item["Key"] for item in page["Contents"]] == ["data/b"]


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
    store.abort_session(session)

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


def test_documents_sit_beside_the_metadata_of_every_type(fake):
    """One store per council, as data/<COUNCIL>/documents/ is locally."""
    stored = document_store(fake).write("2026-01-01-1-s123.pdf", b"%PDF")

    obj = fake.objects["data/KIR/documents/2026-01-01-1-s123.pdf"]
    assert obj["ContentType"] == "application/pdf"
    assert stored.url == "s3://bucket/data/KIR/documents/2026-01-01-1-s123.pdf"
    assert stored.backend == "s3"


def test_replace_leaves_the_documents_beside_it_alone(fake):
    document_store(fake).write("a.pdf", b"%PDF")
    store = metadata_store(fake, mode=StorageMode.REPLACE)
    with store.session("run") as session:
        session.write(Path("json/current.json"), "{}")

    assert "data/KIR/documents/a.pdf" in fake.objects


def test_replace_deletes_anything_else_left_under_its_type(fake):
    """Including a documents/ there, which is no longer where they go."""
    fake.objects["data/KIR/Decisions/documents/old.pdf"] = {"Body": b"%PDF"}
    store = metadata_store(fake, mode=StorageMode.REPLACE)
    with store.session("run") as session:
        session.write(Path("json/current.json"), "{}")

    assert set(fake.objects) == {"data/KIR/Decisions/json/current.json"}


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


def test_one_file_can_be_read_and_checked_without_listing(fake):
    """For looking across hundreds of councils before a run starts."""
    store = metadata_store(fake)
    fake.objects["data/KIR/Decisions/_last-run"] = {
        "Body": b"2026-10-01T10:00:00",
        "Metadata": {},
    }

    assert store.read_now("_last-run") == "2026-10-01T10:00:00"
    assert store.read_now("_index.json") is None
    assert store.exists_now("_last-run")
    assert not store.exists_now("_index.json")
    assert fake.ops("list") == []


def test_the_same_bytes_are_put_once(fake):
    store = document_store(fake)

    first, first_written = store.store(b"%PDF-1.7 report")
    second, second_written = document_store(fake).store(b"%PDF-1.7 report")

    assert first_written and not second_written
    assert second.key == first.key
    assert len(fake.ops("put")) == 1


def test_an_html_page_is_stored_as_html(fake):
    """Not as application/pdf, whatever its link said."""
    stored, _ = document_store(fake).store(b"<!DOCTYPE html><html></html>")

    assert stored.key.endswith(".html")
    obj = fake.objects[f"data/KIR/documents/{stored.key}"]
    assert obj["ContentType"].startswith("text/html")
    assert obj["Metadata"]["sha256"] == stored.key.removesuffix(".html")
