"""
How the minutes scraper handles document files: what it downloads, what it
skips because it already has it, and what it records in the metadata.

No network: every HTTP call is served by a fake client that records what it
was asked for.
"""

import hashlib
import json
from pathlib import Path

import pytest

from lgsf.conf import settings
from lgsf.decisions import DecisionBase
from lgsf.decisions.scrapers import ModGovDecisionsScraper
from lgsf.minutes import MeetingBase
from lgsf.minutes.scrapers import ModGovMinutesScraper
from lgsf.storage.documents.local import LocalDocumentStorage


class FakeResponse:
    def __init__(self, content=b"", status=200, headers=None):
        self.content = content
        self.status_code = status
        self.headers = headers or {}

    @property
    def text(self):
        return self.content.decode("utf-8")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise OSError(f"HTTP {self.status_code}")


class FakeStorageSession:
    """Stand-in for a StorageSession that keeps files in a dict."""

    def __init__(self, files=None):
        self.files = dict(files or {})

    def write(self, filename, content):
        self.files[str(filename)] = content

    def write_bytes(self, filename, content):
        self.files[str(filename)] = content

    def open(self, filename, mode="r"):
        try:
            return self.files[str(filename)]
        except KeyError:
            raise FileNotFoundError(str(filename))


class FakeConsole:
    def __init__(self):
        self.messages = []

    def log(self, message):
        self.messages.append(message)


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    return tmp_path


@pytest.fixture
def scraper():
    """A ModGov minutes scraper with its HTTP layer and storage faked out."""
    scraper = ModGovMinutesScraper.__new__(ModGovMinutesScraper)
    scraper.base_url = "https://democracy.example.gov.uk"
    scraper.council_id = "ABC"
    scraper.options = {}
    scraper.meetings = set()
    scraper.new_data = True
    scraper.unchanged_meetings = 0
    scraper.meetings_saved = 0
    scraper.extra_headers = {}
    scraper.console = FakeConsole()
    scraper.storage_session = FakeStorageSession()
    scraper.document_storage = LocalDocumentStorage(council_code="ABC")

    scraper.requested = []
    scraper.responses = {}

    def fake_get(url, extra_headers=None, raise_for_status=True):
        scraper.requested.append((url, dict(extra_headers or {})))
        return scraper.responses.get(url, FakeResponse(content=b"%PDF-default"))

    scraper.get = fake_get
    return scraper


def content_key_of(content, extension=".pdf"):
    return hashlib.sha256(content).hexdigest() + extension


def make_meeting(documents):
    meeting = MeetingBase(
        "https://democracy.example.gov.uk/ieListDocuments.aspx?CId=1&MId=2",
        identifier="2",
        title="Audit Committee - 2026-08-19",
        committee="Audit Committee",
        date="2026-08-19",
        committee_id="1",
    )
    meeting.documents = documents
    return meeting


def test_every_document_is_stored_not_just_the_minutes(scraper):
    """
    The full agenda pack is kept. Categorisation labels documents; it does
    not decide which ones are worth downloading.
    """
    meeting = make_meeting(
        [
            {
                "url": "https://ex.gov.uk/a.pdf",
                "title": "Minutes",
                "category": "minutes",
            },
            {"url": "https://ex.gov.uk/b.pdf", "title": "Agenda", "category": "agenda"},
            {"url": "https://ex.gov.uk/c.pdf", "title": "Report", "category": "other"},
        ]
    )

    scraper.save_record_documents(meeting)

    assert scraper.documents_downloaded == 3
    assert [url for url, _ in scraper.requested] == [
        "https://ex.gov.uk/a.pdf",
        "https://ex.gov.uk/b.pdf",
        "https://ex.gov.uk/c.pdf",
    ]
    assert all(document["storage_key"] for document in meeting.documents)


def test_document_metadata_records_hash_and_storage_key(scraper):
    scraper.responses["https://ex.gov.uk/a.pdf"] = FakeResponse(
        content=b"%PDF-minutes",
        headers={"etag": '"abc"', "last-modified": "Wed, 19 Aug 2026 00:00:00 GMT"},
    )
    meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )

    scraper.save_record_documents(meeting)

    document = meeting.documents[0]
    assert document["content_hash"].startswith("sha256:")
    assert document["content_length"] == len(b"%PDF-minutes")
    assert document["storage_backend"] == "local"
    assert document["storage_key"] == content_key_of(b"%PDF-minutes")
    assert document["etag"] == '"abc"'
    assert document["last_modified"] == "Wed, 19 Aug 2026 00:00:00 GMT"
    assert scraper.document_storage.read(document["storage_key"]) == b"%PDF-minutes"


def test_documents_already_stored_are_not_downloaded_again(scraper):
    """A second run over the same window reuses what the store holds."""
    meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )
    scraper.save_record_documents(meeting)
    assert scraper.documents_downloaded == 1

    # A second run over the same meeting
    scraper.requested.clear()
    second_meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )
    scraper.save_record_documents(second_meeting)

    assert scraper.requested == []
    assert scraper.documents_skipped == 1
    assert scraper.documents_downloaded == 1


def test_a_skipped_document_still_gets_full_metadata(scraper):
    """Skipping must not produce a thinner JSON record than downloading."""
    first = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )
    scraper.save_record_documents(first)

    second = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )
    scraper.save_record_documents(second)

    for key in ("storage_key", "storage_backend", "content_hash", "content_length"):
        assert second.documents[0][key] == first.documents[0][key]


def test_a_document_missing_from_the_store_is_re_downloaded(scraper):
    """The index is a cache, not the source of truth."""
    meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )
    scraper.save_record_documents(meeting)
    key = meeting.documents[0]["storage_key"]

    # Someone deletes the file but the index still lists it
    Path(scraper.document_storage._path(key)).unlink()
    scraper.requested.clear()

    scraper.save_record_documents(
        make_meeting(
            [
                {
                    "url": "https://ex.gov.uk/a.pdf",
                    "title": "Minutes",
                    "category": "minutes",
                }
            ]
        )
    )

    assert [url for url, _ in scraper.requested] == ["https://ex.gov.uk/a.pdf"]
    assert scraper.document_storage.exists(key)


def test_a_failed_download_does_not_fail_the_scrape(scraper):
    def exploding_get(url, extra_headers=None, raise_for_status=True):
        if "b.pdf" in url:
            raise RuntimeError("connection reset")
        scraper.requested.append((url, dict(extra_headers or {})))
        return FakeResponse(content=b"%PDF-ok")

    scraper.get = exploding_get
    meeting = make_meeting(
        [
            {"url": "https://ex.gov.uk/a.pdf", "title": "A", "category": "other"},
            {"url": "https://ex.gov.uk/b.pdf", "title": "B", "category": "other"},
            {"url": "https://ex.gov.uk/c.pdf", "title": "C", "category": "other"},
        ]
    )

    scraper.save_record_documents(meeting)

    assert scraper.documents_downloaded == 2
    assert scraper.documents_failed == 1
    assert "storage_key" not in meeting.documents[1]
    assert any("Failed to download" in m for m in scraper.console.messages)


def test_a_document_that_is_gone_is_recorded_as_unavailable(scraper):
    """Rather than tried again on every run that reads the meeting."""
    scraper.responses["https://ex.gov.uk/a.pdf"] = FakeResponse(status=404)
    meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )

    scraper.save_record_documents(meeting)

    assert meeting.documents[0]["unavailable"] == 404
    assert "storage_key" not in meeting.documents[0]
    assert scraper.documents_unavailable == 1
    assert scraper.documents_failed == 0


def test_an_empty_document_is_recorded_as_unavailable(scraper, data_dir):
    """A 200 with no body. Fetching it again weeks later gets the same."""
    scraper.responses["https://ex.gov.uk/a.pdf"] = FakeResponse(content=b"")
    meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )

    scraper.save_record_documents(meeting)

    assert meeting.documents[0]["unavailable"] == "empty"
    assert "storage_key" not in meeting.documents[0]
    assert scraper.documents_unavailable == 1
    assert scraper.documents_failed == 0
    assert not (data_dir / "ABC" / "documents").exists()
    assert "https://ex.gov.uk/a.pdf" not in scraper.index["documents"]


def test_documents_are_not_fetched_conditionally(scraper):
    """
    A document is only requested when the store doesn't have it, so a 304
    would only mean asking again without the validators.
    """
    scraper.index["documents"]["https://ex.gov.uk/a.pdf"] = {
        "storage_key": content_key_of(b"%PDF-gone"),
        "etag": '"abc"',
        "last_modified": "Wed, 19 Aug 2026 00:00:00 GMT",
    }
    meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )

    scraper.save_record_documents(meeting)

    assert scraper.requested == [("https://ex.gov.uk/a.pdf", {})]


def test_failed_and_unavailable_documents_are_reported(scraper):
    scraper.responses["https://ex.gov.uk/a.pdf"] = FakeResponse(status=410)
    scraper.responses["https://ex.gov.uk/b.pdf"] = FakeResponse(status=403)
    meeting = make_meeting(
        [
            {"url": "https://ex.gov.uk/a.pdf", "title": "A", "category": "other"},
            {"url": "https://ex.gov.uk/b.pdf", "title": "B", "category": "other"},
        ]
    )
    scraper.save_record_documents(meeting)

    scraper.report()

    assert any("1 unavailable" in m for m in scraper.console.messages)
    assert any("1 documents failed" in m for m in scraper.console.messages)


def test_documents_with_no_url_are_ignored(scraper):
    meeting = make_meeting([{"title": "Broken link", "category": "other"}])
    scraper.save_record_documents(meeting)
    assert scraper.requested == []
    assert scraper.documents_downloaded == 0


def test_different_documents_get_different_keys(scraper):
    """CMIS URLs are opaque, and nothing in the key comes from them."""
    scraper.responses["https://ex.gov.uk/x?token=aaa"] = FakeResponse(b"%PDF-a")
    scraper.responses["https://ex.gov.uk/x?token=bbb"] = FakeResponse(b"%PDF-b")
    meeting = make_meeting(
        [
            {"url": "https://ex.gov.uk/x?token=aaa", "title": "A", "category": "other"},
            {"url": "https://ex.gov.uk/x?token=bbb", "title": "B", "category": "other"},
        ]
    )

    scraper.save_record_documents(meeting)

    keys = [document["storage_key"] for document in meeting.documents]
    assert keys == [content_key_of(b"%PDF-a"), content_key_of(b"%PDF-b")]


def test_a_file_linked_from_minutes_and_decisions_is_stored_once(scraper, data_dir):
    """
    ModernGov links a report from the meeting as mgConvert2PDF.aspx?ID=N
    and from the decision as /documents/sN/<title>.pdf. Each type downloads
    it, since neither can tell from the link that the other has it, but
    only the first writes it.
    """
    meeting_link = "https://democracy.example.gov.uk/mgConvert2PDF.aspx?ID=129839"
    decision_link = (
        "https://democracy.example.gov.uk/documents/s129839/Budget%20report.pdf"
    )
    report = b"%PDF-1.7 budget report"
    scraper.responses[meeting_link] = FakeResponse(report)
    meeting = make_meeting(
        [{"url": meeting_link, "title": "Budget report", "category": "other"}]
    )

    decisions = ModGovDecisionsScraper.__new__(ModGovDecisionsScraper)
    decisions.council_id = "ABC"
    decisions.extra_headers = {}
    decisions.console = FakeConsole()
    decisions.index = {"decisions": {}, "documents": {}, "windows": {}}
    decisions.document_storage = LocalDocumentStorage(council_code="ABC")
    decisions.get = lambda url, **kwargs: FakeResponse(report)
    decision = DecisionBase(
        "https://democracy.example.gov.uk/ieDecisionDetails.aspx?ID=1",
        identifier="1",
        title="Budget",
        date="2026-08-19",
    )
    decision.documents = [{"url": decision_link, "title": "Budget report"}]

    scraper.save_record_documents(meeting)
    decisions.save_record_documents(decision)

    key = content_key_of(report)
    assert [p.name for p in (data_dir / "ABC" / "documents").iterdir()] == [key]
    assert meeting.documents[0]["storage_key"] == key
    assert decision.documents[0]["storage_key"] == key
    assert scraper.documents_downloaded == 1
    assert scraper.documents_already_in_store == 0
    assert decisions.documents_downloaded == 1
    assert decisions.documents_already_in_store == 1


def test_saved_meeting_json_records_where_each_document_went(scraper):
    """
    The metadata is serialised from the document dicts, so documents must be
    stored before the meeting JSON is written - otherwise the JSON that
    lands in git points at nothing.
    """
    meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )

    scraper.process_meeting(meeting, None)

    written = json.loads(
        scraper.storage_session.files["json/2026-08-19-2-audit-committee.json"]
    )
    document = written["documents"][0]
    assert document["storage_key"] == content_key_of(b"%PDF-default")
    assert document["content_hash"].startswith("sha256:")
    assert document["storage_backend"] == "local"


def test_minutes_accumulate_while_councillors_replace():
    """
    Minutes are an append-only historical record; councillors are a current
    set where the latest scrape is the whole truth.
    """
    from lgsf.councillors.scrapers import BaseCouncillorScraper
    from lgsf.minutes.scrapers import BaseMinutesScraper
    from lgsf.storage.backends.base import StorageMode

    assert BaseCouncillorScraper.storage_mode == StorageMode.REPLACE
    assert BaseMinutesScraper.storage_mode == StorageMode.ACCUMULATE


def test_skip_documents_finds_documents_without_downloading_them(scraper):
    """
    --skip-documents exists so a sweep of every council can check that
    scrapers still work without pulling tens of gigabytes of PDFs.
    """
    scraper.options = {"skip_documents": True}
    meeting = make_meeting(
        [
            {
                "url": "https://ex.gov.uk/a.pdf",
                "title": "Minutes",
                "category": "minutes",
            },
            {"url": "https://ex.gov.uk/b.pdf", "title": "Agenda", "category": "agenda"},
        ]
    )

    scraper.process_meeting(meeting, None)

    assert scraper.requested == []
    assert scraper.documents_downloaded == 0
    assert scraper.documents_linked == 2


def test_skip_documents_still_records_the_documents_it_found(scraper):
    """The metadata is the point of the sweep, so it still gets written."""
    scraper.options = {"skip_documents": True}
    meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )

    scraper.process_meeting(meeting, None)

    written = json.loads(
        scraper.storage_session.files["json/2026-08-19-2-audit-committee.json"]
    )
    document = written["documents"][0]
    assert document["url"] == "https://ex.gov.uk/a.pdf"
    assert document["category"] == "minutes"
    # Nothing was downloaded, so there is nothing to point at.
    assert "storage_key" not in document


def test_documents_are_downloaded_by_default(scraper):
    assert scraper.skip_documents is False
    meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )

    scraper.process_meeting(meeting, None)

    assert scraper.documents_downloaded == 1


def test_a_scraper_can_still_opt_out_of_documents_entirely(scraper):
    """save_documents on the class is independent of the CLI flag."""
    scraper.save_documents = False
    meeting = make_meeting(
        [{"url": "https://ex.gov.uk/a.pdf", "title": "Minutes", "category": "minutes"}]
    )

    scraper.process_meeting(meeting, None)

    assert scraper.requested == []
    assert scraper.documents_downloaded == 0
