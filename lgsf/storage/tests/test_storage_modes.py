"""
Storage mode behaviour: StorageMode.REPLACE wipes what came before,
StorageMode.ACCUMULATE adds to it.

This is the difference between councillors (the current set is the whole
truth) and minutes (an append-only historical record), so it is worth
pinning down explicitly for both.
"""

from pathlib import Path

import pytest

from lgsf.conf import settings
from lgsf.storage.backends import get_storage_backend
from lgsf.storage.backends.base import StorageMode
from lgsf.storage.backends.local import LocalFilesystemStorage


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    """Point storage at a temporary directory rather than the repo's data/."""
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    return tmp_path


def stored_files(root):
    return sorted(
        p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
    )


def test_replace_mode_clears_previous_runs(data_dir):
    storage = LocalFilesystemStorage(
        council_code="ABC",
        scraper_object_type="Councillors",
        storage_mode=StorageMode.REPLACE,
    )
    with storage.session("run 1") as session:
        session.write(Path("json/departed-councillor.json"), "{}")

    with storage.session("run 2") as session:
        session.write(Path("json/current-councillor.json"), "{}")

    root = data_dir / "ABC" / "Councillors"
    assert stored_files(root) == ["json/current-councillor.json"]


def test_accumulate_mode_keeps_previous_runs(data_dir):
    storage = LocalFilesystemStorage(
        council_code="ABC",
        scraper_object_type="Minutes",
        storage_mode=StorageMode.ACCUMULATE,
    )
    with storage.session("run 1") as session:
        session.write(Path("json/2026-01-01-old-meeting.json"), "{}")
        session.write_bytes(Path("raw/2026-01-01-old.xml"), b"<xml/>")

    with storage.session("run 2") as session:
        session.write(Path("json/2026-08-01-new-meeting.json"), "{}")

    root = data_dir / "ABC" / "Minutes"
    assert stored_files(root) == [
        "json/2026-01-01-old-meeting.json",
        "json/2026-08-01-new-meeting.json",
        "raw/2026-01-01-old.xml",
    ]


def test_accumulate_mode_still_overwrites_the_same_file(data_dir):
    """Accumulating adds new files; it doesn't stop a meeting being updated."""
    storage = LocalFilesystemStorage(
        council_code="ABC",
        scraper_object_type="Minutes",
        storage_mode=StorageMode.ACCUMULATE,
    )
    with storage.session("run 1") as session:
        session.write(Path("json/meeting.json"), '{"status": "Provisional"}')

    with storage.session("run 2") as session:
        session.write(Path("json/meeting.json"), '{"status": "Confirmed"}')

    root = data_dir / "ABC" / "Minutes"
    assert (root / "json/meeting.json").read_text() == '{"status": "Confirmed"}'


def test_accumulate_mode_can_read_back_earlier_runs(data_dir):
    """A later run must be able to see what an earlier one stored."""
    storage = LocalFilesystemStorage(
        council_code="ABC",
        scraper_object_type="Minutes",
        storage_mode=StorageMode.ACCUMULATE,
    )
    with storage.session("run 1") as session:
        session.write(Path("_index.json"), '{"version": 1}')

    session = storage.start_session()
    assert session.open(Path("_index.json")) == '{"version": 1}'
    storage.end_session(session, "run 2")


def test_replace_mode_cannot_read_back_earlier_runs(data_dir):
    """The mirror image: replacing means last run's state is gone."""
    storage = LocalFilesystemStorage(
        council_code="ABC",
        scraper_object_type="Councillors",
        storage_mode=StorageMode.REPLACE,
    )
    with storage.session("run 1") as session:
        session.write(Path("_index.json"), '{"version": 1}')

    session = storage.start_session()
    with pytest.raises(FileNotFoundError):
        session.open(Path("_index.json"))
    storage.end_session(session, "run 2")


def test_default_mode_is_replace(data_dir):
    assert (
        LocalFilesystemStorage(council_code="ABC").storage_mode == StorageMode.REPLACE
    )


def test_unknown_storage_mode_is_rejected(data_dir):
    with pytest.raises(ValueError, match="Unknown storage_mode"):
        LocalFilesystemStorage(council_code="ABC", storage_mode="append")


def test_factory_passes_storage_mode_through(data_dir):
    storage = get_storage_backend(
        council_code="ABC",
        options={},
        scraper_object_type="Minutes",
        storage_mode=StorageMode.ACCUMULATE,
    )
    assert storage.storage_mode == StorageMode.ACCUMULATE
