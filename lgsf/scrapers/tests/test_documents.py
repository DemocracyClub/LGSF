"""
Saving the documents a record links to, as any data type does it.
"""

import hashlib

import pytest

from lgsf.conf import settings
from lgsf.decisions.scrapers import ModGovDecisionsScraper
from lgsf.minutes.scrapers import ModGovMinutesScraper
from lgsf.scrapers.base import ScraperBase
from lgsf.scrapers.documents import DocumentSavingMixin
from lgsf.storage import s3
from lgsf.storage.tests.test_s3_storage import FakeS3


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


# ---- one store per council ----


def scraper_for(scraper_class, options):
    scraper = scraper_class.__new__(scraper_class)
    scraper.council_id = "ABC"
    scraper.options = options
    return scraper


def test_every_type_of_a_council_shares_one_local_store(data_dir):
    minutes = scraper_for(ModGovMinutesScraper, {"council": "ABC"})
    decisions = scraper_for(ModGovDecisionsScraper, {"council": "ABC"})

    assert minutes.document_storage.root == decisions.document_storage.root
    assert minutes.document_storage.root == (data_dir / "ABC" / "documents").resolve()


def test_every_type_of_a_council_shares_one_s3_store(monkeypatch):
    """As locally: documents on S3 used to sit under each type's prefix."""
    bucket = FakeS3()
    monkeypatch.setattr(s3, "get_client", lambda: bucket)
    monkeypatch.setenv("LGSF_S3_BUCKET", "bucket")
    monkeypatch.setenv("LGSF_S3_PREFIX", "data")
    options = {"council": "ABC", "document_storage_backend": "s3"}
    minutes = scraper_for(ModGovMinutesScraper, options)
    decisions = scraper_for(ModGovDecisionsScraper, options)

    stored, _ = minutes.document_storage.store(b"%PDF-1.7 report")

    assert minutes.document_storage.root == "data/ABC/documents"
    assert decisions.document_storage.root == "data/ABC/documents"
    assert decisions.document_storage.exists(stored.key)


# --- documents another data type already stored ---


import json  # noqa: E402

from lgsf.storage.backends.base import StorageMode  # noqa: E402
from lgsf.storage.backends.local import LocalFilesystemStorage  # noqa: E402

SHARED = b"%PDF-1.7 the cabinet report"
SHARED_URL = "https://ex.gov.uk/mgConvert2PDF.aspx?ID=1"


class MinutesLikeScraper(DocumentSavingMixin, ScraperBase):
    service_name = "minutes"
    scraper_object_type = "Minutes"


def stored_by_decisions(data_dir, url=SHARED_URL, content=SHARED, store_file=True):
    """Decisions has fetched a document and recorded it in its index."""
    key = hashlib.sha256(content).hexdigest() + ".pdf"
    if store_file:
        (data_dir / "ABC" / "documents").mkdir(parents=True, exist_ok=True)
        (data_dir / "ABC" / "documents" / key).write_bytes(content)
    entry = {
        "storage_key": key,
        "storage_backend": "local",
        "content_hash": f"sha256:{hashlib.sha256(content).hexdigest()}",
        "content_length": len(content),
        "etag": '"d1"',
    }
    (data_dir / "ABC" / "Decisions").mkdir(parents=True, exist_ok=True)
    (data_dir / "ABC" / "Decisions" / "_index.json").write_text(
        json.dumps({"decisions": {}, "documents": {url: entry}})
    )
    return entry


def minutes_scraper(responses):
    scraper = MinutesLikeScraper.__new__(MinutesLikeScraper)
    scraper.council_id = "ABC"
    scraper.options = {"council": "ABC"}
    scraper.console = FakeConsole()
    scraper.index = {"documents": {}}
    scraper.requests = []
    scraper.storage_backend = LocalFilesystemStorage(
        "ABC", scraper_object_type="Minutes", storage_mode=StorageMode.ACCUMULATE
    )

    def send(url, extra_headers=None):
        scraper.requests.append(url)
        return responses[url]

    scraper._send = send
    return scraper


def test_a_document_decisions_stored_is_not_downloaded_for_minutes(data_dir):
    entry = stored_by_decisions(data_dir)
    scraper = minutes_scraper({SHARED_URL: FakeResponse(SHARED)})
    meeting = Thing(SHARED_URL)

    scraper.save_record_documents(meeting)

    assert scraper.requests == []
    assert meeting.documents[0]["storage_key"] == entry["storage_key"]
    assert scraper.index["documents"][SHARED_URL]["storage_key"] == entry["storage_key"]
    assert (scraper.documents_skipped, scraper.documents_from_other_types) == (1, 1)


def test_another_types_entry_whose_file_is_gone_is_downloaded(data_dir):
    stored_by_decisions(data_dir, store_file=False)
    scraper = minutes_scraper({SHARED_URL: FakeResponse(SHARED)})

    scraper.save_record_documents(Thing(SHARED_URL))

    assert scraper.requests == [SHARED_URL]
    assert scraper.documents_from_other_types == 0


def test_this_types_own_entry_comes_first(data_dir):
    stored_by_decisions(data_dir)
    scraper = minutes_scraper({})
    own = dict(scraper.other_types_documents()[SHARED_URL], etag='"mine"')
    scraper.index["documents"][SHARED_URL] = own

    assert scraper.stored_document_metadata(SHARED_URL)["etag"] == '"mine"'
    assert scraper.documents_from_other_types == 0


def test_an_unusable_own_entry_falls_back_to_another_types(data_dir):
    """An old-style key in this type's index, a good one in decisions'."""
    entry = stored_by_decisions(data_dir)
    scraper = minutes_scraper({})
    scraper.index["documents"][SHARED_URL] = {"storage_key": "2024-01-01-old.pdf"}

    found = scraper.stored_document_metadata(SHARED_URL)

    assert found["storage_key"] == entry["storage_key"]
    assert scraper.index["documents"][SHARED_URL]["storage_key"] == entry["storage_key"]


def test_the_other_types_indexes_are_read_once_per_run(data_dir, monkeypatch):
    stored_by_decisions(data_dir)
    scraper = minutes_scraper({})
    reads = []
    real = scraper.storage_backend.read_other_type
    monkeypatch.setattr(
        scraper.storage_backend,
        "read_other_type",
        lambda t, f: reads.append(t) or real(t, f),
    )

    scraper.save_record_documents(Thing(SHARED_URL))
    scraper.save_record_documents(Thing(SHARED_URL, "https://ex.gov.uk/other.pdf"))

    assert sorted(reads) == ["Councillors", "Decisions"]


def test_a_scraper_without_a_storage_backend_just_downloads():
    scraper = make_scraper({"https://ex.gov.uk/a.pdf": FakeResponse(b"%PDF-1.7 x")})

    scraper.save_record_documents(Thing("https://ex.gov.uk/a.pdf"))

    assert scraper.documents_downloaded == 1
