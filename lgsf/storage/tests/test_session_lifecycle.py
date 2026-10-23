"""
How a storage session ends: commit, abort, close.

end_session commits and runs only when a run is good. abort_session
commits nothing and is the whole of the failure path. close_session runs
after either, however they went, so a backend's cleanup has one home.
Checked against the local backend, with the hooks recorded.
"""

from pathlib import Path

import pytest

from lgsf.conf import settings
from lgsf.storage.backends.base import StorageMode
from lgsf.storage.backends.local import LocalFilesystemStorage


class RecordingStorage(LocalFilesystemStorage):
    def __init__(self, *args, fail_commit=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = []
        self.fail_commit = fail_commit

    def _end_session(self, session, commit_message, **kwargs):
        self.calls.append("end")
        if self.fail_commit:
            raise OSError("disk full")
        return super()._end_session(session, commit_message, **kwargs)

    def _abort_session(self, session):
        self.calls.append("abort")

    def _close_session(self, session):
        self.calls.append("close")
        super()._close_session(session)


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    return tmp_path


def storage(**kwargs):
    return RecordingStorage("ABC", scraper_object_type="Councillors", **kwargs)


def stored(data_dir):
    root = data_dir / "ABC" / "Councillors"
    return sorted(
        p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
    )


def test_a_good_run_commits_then_closes(data_dir):
    store = storage()
    session = store.start_session()
    session.write(Path("json/a.json"), "{}")

    store.end_session(session, "run")

    assert store.calls == ["end", "close"]
    assert stored(data_dir) == ["json/a.json"]
    assert store._active is None


def test_an_aborted_run_commits_nothing_then_closes(data_dir):
    store = storage()
    session = store.start_session()
    session.write(Path("json/a.json"), "{}")

    store.abort_session(session)

    assert store.calls == ["abort", "close"]
    assert stored(data_dir) == []
    assert store._active is None


def test_close_runs_even_when_the_commit_itself_fails(data_dir):
    store = storage(fail_commit=True)
    session = store.start_session()
    session.write(Path("json/a.json"), "{}")

    with pytest.raises(OSError, match="disk full"):
        store.end_session(session, "run")

    assert store.calls == ["end", "close"]
    assert store._active is None
    store.start_session()  # a new session can start


def test_the_context_manager_aborts_on_an_exception_and_commits_otherwise(data_dir):
    store = storage()
    with pytest.raises(ValueError):
        with store.session("run 1") as session:
            session.write(Path("json/partial.json"), "{}")
            raise ValueError("page looked wrong")
    assert store.calls == ["abort", "close"]
    assert stored(data_dir) == []

    store.calls.clear()
    with store.session("run 2") as session:
        session.write(Path("json/a.json"), "{}")
    assert store.calls == ["end", "close"]
    assert stored(data_dir) == ["json/a.json"]


def test_a_replace_run_that_aborts_leaves_the_previous_commit(data_dir):
    store = storage(storage_mode=StorageMode.REPLACE)
    with store.session("run 1") as session:
        session.write(Path("json/departed.json"), "{}")
    session = store.start_session()
    session.write(Path("json/current.json"), "{}")

    store.abort_session(session)

    assert stored(data_dir) == ["json/departed.json"]
