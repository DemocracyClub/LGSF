"""The local and S3 stores, the S3 one against the storage tests' fake client."""

import datetime
import pickle

import pytest

from lgsf.embeddings.stores import LocalStore, S3Store, Source, store_for
from lgsf.storage.tests.test_s3_storage import FakeS3

DIGEST = "a" * 64
KEY = f"STO/documents/{DIGEST}.pdf"


@pytest.fixture
def local(tmp_path):
    return LocalStore(tmp_path)


@pytest.fixture
def s3():
    return S3Store("lgsf-data-archive", "merged-data", client=FakeS3(page_size=2))


@pytest.fixture(params=["local", "s3"])
def store(request):
    return request.getfixturevalue(request.param)


def test_write_and_read(store):
    assert not store.exists(KEY)
    store.write_bytes(KEY, b"%PDF-1.7\n")
    assert store.exists(KEY)
    assert store.read_bytes(KEY) == b"%PDF-1.7\n"


def test_read_missing(store):
    with pytest.raises(FileNotFoundError):
        store.read_bytes(KEY)


def test_list_prefix(store):
    for name in ("b.pdf", "a.pdf", "a.pdf.status.json"):
        store.write_bytes(f"STO/documents/{name}", b"")
    store.write_bytes("STOX/documents/c.pdf", b"")
    assert store.list_prefix("STO/documents/") == [
        "STO/documents/a.pdf",
        "STO/documents/a.pdf.status.json",
        "STO/documents/b.pdf",
    ]
    assert store.list_prefix("STO/documents/a.pdf.") == [
        "STO/documents/a.pdf.status.json",
    ]
    assert store.list_prefix("OLD/documents/") == []


def test_list_prefix_stops_at_the_slash(store):
    store.write_bytes("STO/documents/a.pdf", b"")
    store.write_bytes("STO/documents-backup/a.pdf", b"")
    store.write_bytes("STO/documents.old", b"")
    assert store.list_prefix("STO/documents/") == ["STO/documents/a.pdf"]


def test_list_sizes(store):
    store.write_bytes("STO/documents/a.pdf", b"12345")
    store.write_bytes("STO/documents/a.pdf.md", b"")
    assert store.list_sizes("STO/documents/") == {
        "STO/documents/a.pdf": 5,
        "STO/documents/a.pdf.md": 0,
    }


def test_list_entries(store):
    before = datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=2)
    store.write_bytes("STO/documents/a.pdf", b"12345")
    entries = store.list_entries("STO/documents/")
    assert list(entries) == ["STO/documents/a.pdf"]
    entry = entries["STO/documents/a.pdf"]
    assert entry.size == 5
    assert entry.last_modified.tzinfo is not None
    assert before <= entry.last_modified <= datetime.datetime.now(datetime.UTC)


def test_download_to(store, tmp_path):
    store.write_bytes(KEY, b"%PDF-1.7\n")
    path = store.download_to(KEY, tmp_path / "work")
    assert path.read_bytes() == b"%PDF-1.7\n"


def test_download_missing(store, tmp_path):
    with pytest.raises(FileNotFoundError):
        store.download_to(KEY, tmp_path)


def test_local_writes_leave_no_temporary_files(local, tmp_path):
    local.write_bytes(KEY, b"one")
    local.write_bytes(KEY, b"two")
    assert [p.name for p in (tmp_path / "STO" / "documents").iterdir()] == [
        f"{DIGEST}.pdf"
    ]
    assert local.read_bytes(KEY) == b"two"


def test_local_download_is_the_file_itself(local, tmp_path):
    local.write_bytes(KEY, b"%PDF-1.7\n")
    assert local.download_to(KEY, tmp_path / "elsewhere") == tmp_path / KEY


def test_s3_keys_are_under_the_prefix(s3):
    s3.write_bytes(KEY, b"")
    assert list(s3.client.objects) == [f"merged-data/{KEY}"]
    assert s3.url(KEY) == f"s3://lgsf-data-archive/merged-data/{KEY}"


def test_s3_writes_carry_a_content_type(s3):
    s3.write_bytes(KEY + ".status.json", b"{}")
    s3.write_bytes(KEY + ".md", b"# Hello")
    objects = s3.client.objects
    assert objects[f"merged-data/{KEY}.status.json"]["ContentType"] == (
        "application/json; charset=utf-8"
    )
    assert objects[f"merged-data/{KEY}.md"]["ContentType"].startswith("text/markdown")


def test_s3_store_without_a_prefix():
    store = S3Store("bucket", "", client=FakeS3())
    store.write_bytes(KEY, b"")
    assert list(store.client.objects) == [KEY]
    assert store.list_prefix("STO/documents/") == [KEY]


def test_s3_store_is_pickled_without_its_client(s3):
    copy = pickle.loads(pickle.dumps(s3))
    assert copy._client is None
    assert (copy.bucket, copy.prefix) == ("lgsf-data-archive", "merged-data")


def test_s3_store_from_the_environment(monkeypatch):
    monkeypatch.setenv("LGSF_S3_BUCKET", "lgsf-data-archive")
    monkeypatch.setenv("LGSF_S3_PREFIX", "merged-data/")
    monkeypatch.setattr("lgsf.storage.s3.get_client", lambda: FakeS3())
    store = store_for(Source.S3)
    assert isinstance(store, S3Store)
    assert (store.bucket, store.prefix) == ("lgsf-data-archive", "merged-data")
    assert isinstance(store.client, FakeS3)


def test_s3_store_needs_a_bucket(monkeypatch):
    monkeypatch.delenv("LGSF_S3_BUCKET", raising=False)
    with pytest.raises(ValueError, match="LGSF_S3_BUCKET"):
        store_for(Source.S3)


def test_local_store_from_the_data_directory(tmp_path):
    assert store_for(Source.LOCAL, tmp_path).root == tmp_path
    assert store_for(Source.LOCAL).root.name == "data"
