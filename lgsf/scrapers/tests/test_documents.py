"""
Saving the documents a record links to, as any data type does it.
"""

import hashlib

import pytest

from lgsf.conf import settings
from lgsf.scrapers.base import ScraperBase
from lgsf.scrapers.documents import DocumentSavingMixin


class FakeConsole:
    def __init__(self):
        self.messages = []

    def log(self, message):
        self.messages.append(message)


class FakeResponse:
    def __init__(self, content=b"", status_code=200, headers=None):
        self.content = content
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise OSError(f"HTTP {self.status_code}")


class ThingScraper(DocumentSavingMixin, ScraperBase):
    """A new data type, built from what the framework gives it."""

    service_name = "things"
    scraper_object_type = "Things"


class Thing:
    def __init__(self, *urls):
        self.documents = [{"url": url, "title": "A thing"} for url in urls]


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    monkeypatch.delenv("LGSF_DOCUMENT_STORAGE_BACKEND", raising=False)
    monkeypatch.delenv("LGSF_STORAGE_BACKEND", raising=False)
    return tmp_path


def make_scraper(responses):
    """
    A ThingScraper with its HTTP layer faked. ScraperBase.__init__ wants
    council metadata, which a test of saving documents doesn't need.
    """
    scraper = ThingScraper.__new__(ThingScraper)
    scraper.council_id = "ABC"
    scraper.options = {"council": "ABC"}
    scraper.console = FakeConsole()
    scraper.index = {"documents": {}}
    scraper.requests = []

    def send(url, extra_headers=None):
        scraper.requests.append(url)
        return responses[url]

    scraper._send = send
    return scraper


def test_a_new_type_saves_documents_with_the_mixin(data_dir):
    scraper = make_scraper(
        {"https://ex.gov.uk/a.pdf": FakeResponse(b"%PDF-1.7 a thing")}
    )
    thing = Thing("https://ex.gov.uk/a.pdf")

    missing = scraper.save_record_documents(thing)

    key = hashlib.sha256(b"%PDF-1.7 a thing").hexdigest() + ".pdf"
    assert missing == 0
    assert thing.documents[0]["storage_key"] == key
    assert scraper.index["documents"]["https://ex.gov.uk/a.pdf"]["storage_key"] == key
    assert (data_dir / "ABC" / "documents" / key).read_bytes() == b"%PDF-1.7 a thing"
    assert scraper.documents_downloaded == 1


def test_a_document_in_the_index_is_not_downloaded_again():
    scraper = make_scraper(
        {"https://ex.gov.uk/a.pdf": FakeResponse(b"%PDF-1.7 a thing")}
    )
    scraper.save_record_documents(Thing("https://ex.gov.uk/a.pdf"))

    again = Thing("https://ex.gov.uk/a.pdf")
    scraper.save_record_documents(again)

    assert scraper.requests == ["https://ex.gov.uk/a.pdf"]
    assert scraper.documents_skipped == 1
    assert again.documents[0]["storage_key"]


def test_failures_are_counted_and_returned():
    scraper = make_scraper(
        {
            "https://ex.gov.uk/gone.pdf": FakeResponse(status_code=404),
            "https://ex.gov.uk/broken.pdf": FakeResponse(status_code=500),
        }
    )
    thing = Thing("https://ex.gov.uk/gone.pdf", "https://ex.gov.uk/broken.pdf")

    missing = scraper.save_record_documents(thing)

    assert missing == 1
    assert scraper.documents_failed == 1
    assert scraper.documents_unavailable == 1
    assert thing.documents[0]["unavailable"] == 404
    assert "storage_key" not in thing.documents[1]


def test_the_counters_start_at_zero_without_an_init():
    scraper = make_scraper({})
    for counter in (
        "documents_linked",
        "documents_downloaded",
        "documents_skipped",
        "documents_failed",
        "documents_unavailable",
        "documents_bytes",
        "documents_already_in_store",
    ):
        assert getattr(scraper, counter) == 0
