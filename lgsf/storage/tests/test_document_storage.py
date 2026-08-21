"""Document storage backend behaviour."""

from pathlib import Path

import pytest

from lgsf.conf import settings
from lgsf.storage.documents import (
    get_available_document_backends,
    get_document_storage_backend,
    hash_content,
)
from lgsf.storage.documents.local import LocalDocumentStorage


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    return tmp_path


def test_documents_are_stored_under_the_council_documents_dir(data_dir):
    storage = LocalDocumentStorage(council_code="ABC")
    stored = storage.write("2026-08-19-council-1234.pdf", b"%PDF-fake")

    expected = data_dir / "ABC" / "documents" / "2026-08-19-council-1234.pdf"
    assert expected.is_file()
    assert expected.read_bytes() == b"%PDF-fake"
    assert stored.key == "2026-08-19-council-1234.pdf"
    assert stored.url == expected.resolve().as_uri()
    assert stored.backend == "local"


def test_stored_document_records_hash_and_length(data_dir):
    storage = LocalDocumentStorage(council_code="ABC")
    stored = storage.write("doc.pdf", b"%PDF-fake")

    assert stored.content_hash == hash_content(b"%PDF-fake")
    assert stored.content_hash.startswith("sha256:")
    assert stored.content_length == len(b"%PDF-fake")


def test_as_dict_is_what_gets_recorded_in_meeting_metadata(data_dir):
    storage = LocalDocumentStorage(council_code="ABC")
    stored = storage.write("doc.pdf", b"%PDF-fake")

    assert stored.as_dict() == {
        "storage_key": "doc.pdf",
        "storage_backend": "local",
        "content_hash": stored.content_hash,
        "content_length": 9,
    }


def test_metadata_does_not_record_a_machine_specific_path(data_dir):
    """
    The resolved URL contains an absolute filesystem path, which differs
    for every developer. Metadata goes to git, so it must not carry one.
    """
    storage = LocalDocumentStorage(council_code="ABC")
    stored = storage.write("doc.pdf", b"content")

    assert str(data_dir) in stored.url
    assert not any(str(data_dir) in str(v) for v in stored.as_dict().values())


def test_the_backend_can_rebuild_the_url_from_the_stored_key(data_dir):
    """The key is what gets recorded; the URL resolves from it on demand."""
    storage = LocalDocumentStorage(council_code="ABC")
    stored = storage.write("doc.pdf", b"content")

    recorded = stored.as_dict()
    assert storage.url_for(recorded["storage_key"]) == stored.url


def test_exists_and_read_round_trip(data_dir):
    storage = LocalDocumentStorage(council_code="ABC")
    assert storage.exists("doc.pdf") is False

    storage.write("doc.pdf", b"content")
    assert storage.exists("doc.pdf") is True
    assert storage.read("doc.pdf") == b"content"


def test_read_missing_document_raises(data_dir):
    storage = LocalDocumentStorage(council_code="ABC")
    with pytest.raises(FileNotFoundError):
        storage.read("nope.pdf")


def test_describe_returns_none_for_missing_document(data_dir):
    storage = LocalDocumentStorage(council_code="ABC")
    assert storage.describe("nope.pdf") is None


def test_describe_rebuilds_metadata_for_a_stored_document(data_dir):
    storage = LocalDocumentStorage(council_code="ABC")
    written = storage.write("doc.pdf", b"content")

    described = storage.describe("doc.pdf")
    assert described == written


def test_documents_are_isolated_per_council(data_dir):
    LocalDocumentStorage(council_code="ABC").write("doc.pdf", b"abc")
    LocalDocumentStorage(council_code="XYZ").write("doc.pdf", b"xyz")

    assert (data_dir / "ABC" / "documents" / "doc.pdf").read_bytes() == b"abc"
    assert (data_dir / "XYZ" / "documents" / "doc.pdf").read_bytes() == b"xyz"


def test_documents_sit_beside_not_inside_the_metadata_dirs(data_dir):
    """
    Documents are shared across scraper types and excluded from version
    control, so they must not land inside data/<COUNCIL>/<Type>/.
    """
    storage = LocalDocumentStorage(council_code="ABC")
    storage.write("doc.pdf", b"x")

    assert (data_dir / "ABC" / "documents").is_dir()
    assert not (data_dir / "ABC" / "Minutes").exists()


def test_writes_are_atomic_leaving_no_temp_files(data_dir):
    storage = LocalDocumentStorage(council_code="ABC")
    storage.write("nested/dir/doc.pdf", b"x")

    files = [p.name for p in (data_dir / "ABC" / "documents").rglob("*") if p.is_file()]
    assert files == ["doc.pdf"]


def test_overwriting_a_key_replaces_the_content(data_dir):
    storage = LocalDocumentStorage(council_code="ABC")
    storage.write("doc.pdf", b"first")
    stored = storage.write("doc.pdf", b"second")

    assert storage.read("doc.pdf") == b"second"
    assert stored.content_hash == hash_content(b"second")


@pytest.mark.parametrize(
    "key", ["../escape.pdf", "/absolute.pdf", "nested/../../escape.pdf", ""]
)
def test_unsafe_keys_are_rejected(data_dir, key):
    storage = LocalDocumentStorage(council_code="ABC")
    with pytest.raises(ValueError):
        storage.write(key, b"x")


def test_factory_defaults_to_local(data_dir):
    storage = get_document_storage_backend(council_code="ABC", options={})
    assert isinstance(storage, LocalDocumentStorage)


def test_factory_honours_an_explicit_option(data_dir):
    storage = get_document_storage_backend(
        council_code="ABC", options={"document_storage_backend": "local"}
    )
    assert isinstance(storage, LocalDocumentStorage)


def test_factory_honours_the_environment_variable(data_dir, monkeypatch):
    monkeypatch.setenv("LGSF_DOCUMENT_STORAGE_BACKEND", "local")
    storage = get_document_storage_backend(council_code="ABC", options={})
    assert isinstance(storage, LocalDocumentStorage)


def test_factory_rejects_unknown_backends(data_dir):
    with pytest.raises(ValueError, match="Unsupported document storage backend"):
        get_document_storage_backend(council_code="ABC", backend_type="magic")


def test_document_backend_is_independent_of_metadata_backend(data_dir, monkeypatch):
    """
    The whole point of the split: metadata can go to git while documents go
    somewhere else, so selecting one must not select the other.
    """
    monkeypatch.setenv("LGSF_STORAGE_BACKEND", "github")
    storage = get_document_storage_backend(council_code="ABC", options={})
    assert isinstance(storage, LocalDocumentStorage)


def test_available_backends_listed(data_dir):
    assert get_available_document_backends() == ["local"]


def test_hash_content_is_stable(data_dir):
    assert hash_content(b"x") == hash_content(b"x")
    assert hash_content(b"x") != hash_content(b"y")


def test_empty_council_code_rejected(data_dir):
    with pytest.raises(ValueError):
        LocalDocumentStorage(council_code="  ")


def test_url_for_works_without_the_file_existing(data_dir):
    storage = LocalDocumentStorage(council_code="ABC")
    url = storage.url_for("doc.pdf")
    assert url.startswith("file://")
    assert url.endswith("/ABC/documents/doc.pdf")
    assert not Path(storage._path("doc.pdf")).exists()
