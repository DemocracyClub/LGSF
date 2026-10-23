"""
Not losing a long minutes run to an interruption.

Drives a minutes scraper against the real local storage backends in a
temporary directory, with only the HTTP layer faked.
"""

import json

import pytest

from lgsf.conf import settings
from lgsf.minutes.scrapers import BaseMinutesScraper
from lgsf.scrapers.base import ScraperBase
from lgsf.storage.backends.base import StorageMode
from lgsf.storage.backends.local import LocalFilesystemStorage

BASE_URL = "https://democracy.example.gov.uk"
MEETING_IDS = [str(i) for i in range(1, 8)]


class FakeConsole:
    def log(self, *args, **kwargs):
        pass


class FakeResponse:
    def __init__(self, content=b"", status_code=200):
        self.content = content
        self.status_code = status_code
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise OSError(f"HTTP {self.status_code}")


class ListedMinutesScraper(BaseMinutesScraper):
    """Seven meetings, each fetched from its own page with one document."""

    def get_meetings(self):
        return [{"id": i} for i in MEETING_IDS]

    def get_single_meeting(self, row):
        url = f"{BASE_URL}/meeting/{row['id']}"
        self.get(url)
        meeting = self.add_meeting(
            url,
            identifier=row["id"],
            title=f"Council - {row['id']}",
            committee="Council",
            date="2026-01-01",
        )
        meeting.documents = [
            {"url": document_url(row["id"]), "title": "Minutes", "category": "minutes"}
        ]
        return meeting


def document_url(meeting_id):
    return f"{BASE_URL}/documents/s{meeting_id}/minutes.pdf"


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    monkeypatch.delenv("LGSF_DOCUMENT_STORAGE_BACKEND", raising=False)
    monkeypatch.delenv("LGSF_STORAGE_BACKEND", raising=False)
    return tmp_path


@pytest.fixture
def make_scraper(monkeypatch):
    """A minutes scraper on real local storage, answering from route(url)."""

    def base_init(self, options, console):
        self.options = options
        self.console = console
        self.council_id = options["council"]
        self.base_url = BASE_URL
        self.storage_backend = LocalFilesystemStorage(
            council_code=self.council_id,
            scraper_object_type="Minutes",
            storage_mode=StorageMode.ACCUMULATE,
        )
        self.storage_session = self.storage_backend.start_session()

    monkeypatch.setattr(ScraperBase, "__init__", base_init)

    def build(route):
        scraper = ListedMinutesScraper({"council": "TST"}, FakeConsole())
        scraper.requests = []

        def send(url, extra_headers=None):
            scraper.requests.append(url)
            return route(url)

        scraper._send = send
        return scraper

    return build


def answer(url):
    if "/documents/" in url:
        return FakeResponse(f"%PDF-1.7 {url}".encode())
    return FakeResponse(b"<html></html>")


def interrupted_at(meeting_id):
    def route(url):
        if url == f"{BASE_URL}/meeting/{meeting_id}":
            raise KeyboardInterrupt
        return answer(url)

    return route


def stored_index(data_dir):
    return json.loads((data_dir / "TST" / "Minutes" / "_index.json").read_text())


def stored_meetings(data_dir):
    return sorted(p.name for p in (data_dir / "TST" / "Minutes" / "json").iterdir())


def test_an_interrupted_run_keeps_what_it_saved_before_its_last_checkpoint(
    make_scraper, data_dir
):
    scraper = make_scraper(interrupted_at("6"))
    scraper.checkpoint_every = 2
    with pytest.raises(KeyboardInterrupt):
        scraper.run(run_log=None)

    # Checkpoints after the second and fourth meetings. The fifth was saved
    # in a session the interruption never committed.
    assert stored_meetings(data_dir) == [
        f"2026-01-01-{i}-council.json" for i in ("1", "2", "3", "4")
    ]
    documents = stored_index(data_dir)["documents"]
    assert set(documents) == {document_url(i) for i in ("1", "2", "3", "4")}
    for entry in documents.values():
        assert (data_dir / "TST" / "documents" / entry["storage_key"]).is_file()


def test_a_rerun_does_not_download_the_documents_checkpointed_before(
    make_scraper, data_dir
):
    first = make_scraper(interrupted_at("6"))
    first.checkpoint_every = 2
    with pytest.raises(KeyboardInterrupt):
        first.run(run_log=None)

    again = make_scraper(answer)
    again.run(run_log=None)

    downloaded = [u for u in again.requests if "/documents/" in u]
    assert downloaded == [document_url(i) for i in ("5", "6", "7")]
    assert again.documents_skipped == 4
    # The fifth meeting's document was stored by the first run but never
    # named in a saved index, so it is downloaded but not written again.
    assert again.documents_already_in_store == 1


def test_checkpoints_are_skipped_where_commits_are_expensive(make_scraper):
    scraper = make_scraper(answer)
    scraper.checkpoint_every = 1
    scraper.storage_backend.supports_checkpoints = False
    ended = []
    original = scraper.storage_backend.end_session
    scraper.storage_backend.end_session = lambda *a, **k: (
        ended.append(a),
        original(*a, **k),
    )[1]

    scraper.run(run_log=None)

    assert len(ended) == 1


def test_minutes_checkpoint_every_fifty_meetings_by_default():
    assert BaseMinutesScraper.checkpoint_every == 50
