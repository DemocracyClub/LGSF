"""
The run index and conditional requests: how a run avoids re-fetching what
hasn't changed since the last one.
"""

import json

import pytest
from bs4 import BeautifulSoup

from lgsf.conf import settings
from lgsf.minutes.exceptions import MeetingNotModifiedException
from lgsf.minutes.scrapers import ModGovMinutesScraper
from lgsf.minutes.tests.test_document_scraping import (
    FakeConsole,
    FakeResponse,
    FakeStorageSession,
)
from lgsf.storage.documents.local import LocalDocumentStorage


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    return tmp_path


def make_scraper(stored_files=None):
    scraper = ModGovMinutesScraper.__new__(ModGovMinutesScraper)
    scraper.base_url = "https://democracy.example.gov.uk"
    scraper.council_id = "ABC"
    scraper.options = {}
    scraper.meetings = set()
    scraper.new_data = True
    scraper.unchanged_meetings = 0
    scraper.documents_downloaded = 0
    scraper.documents_skipped = 0
    scraper.documents_linked = 0
    scraper.extra_headers = {}
    scraper.console = FakeConsole()
    scraper.storage_session = FakeStorageSession(stored_files)
    scraper.document_storage = LocalDocumentStorage(council_code="ABC")
    scraper.requested = []
    scraper.responses = {}

    def fake_get(url, extra_headers=None):
        scraper.requested.append((url, dict(extra_headers or {})))
        return scraper.responses.get(url, FakeResponse(content=b"<meeting/>"))

    scraper.get = fake_get
    return scraper


def test_index_starts_empty_when_nothing_is_stored():
    scraper = make_scraper()
    assert scraper.index["meetings"] == {}
    assert scraper.index["documents"] == {}


def test_index_is_read_back_from_storage():
    stored = {
        "_index.json": json.dumps(
            {
                "version": 1,
                "meetings": {"https://ex.gov.uk/m/1": {"etag": '"abc"'}},
                "documents": {},
            }
        )
    }
    scraper = make_scraper(stored)
    assert scraper.index["meetings"]["https://ex.gov.uk/m/1"]["etag"] == '"abc"'


def test_an_index_from_a_different_version_is_discarded():
    """Better to re-fetch than to guess at an unfamiliar index shape."""
    stored = {
        "_index.json": json.dumps(
            {"version": 99, "meetings": {"https://ex.gov.uk/m/1": {"etag": '"abc"'}}}
        )
    }
    scraper = make_scraper(stored)
    assert scraper.index["meetings"] == {}


def test_a_corrupt_index_is_discarded():
    scraper = make_scraper({"_index.json": "not json at all"})
    assert scraper.index["meetings"] == {}


def test_save_index_writes_it_back():
    scraper = make_scraper()
    scraper.index["meetings"]["https://ex.gov.uk/m/1"] = {"etag": '"abc"'}
    scraper.save_index()

    written = json.loads(scraper.storage_session.files["_index.json"])
    assert written["version"] == 1
    assert written["meetings"]["https://ex.gov.uk/m/1"]["etag"] == '"abc"'
    assert "updated" in written


def test_validators_for_returns_stored_values():
    scraper = make_scraper()
    scraper.index["meetings"]["https://ex.gov.uk/m/1"] = {
        "etag": '"abc"',
        "last_modified": "Wed, 19 Aug 2026 00:00:00 GMT",
    }
    assert scraper.validators_for("https://ex.gov.uk/m/1") == (
        '"abc"',
        "Wed, 19 Aug 2026 00:00:00 GMT",
    )


def test_validators_for_unknown_url_is_empty():
    scraper = make_scraper()
    assert scraper.validators_for("https://ex.gov.uk/nope") == (None, None)


def test_record_source_captures_validators_from_the_response():
    scraper = make_scraper()
    response = FakeResponse(
        headers={"etag": '"xyz"', "last-modified": "Wed, 19 Aug 2026 00:00:00 GMT"}
    )

    source = scraper.record_source("https://ex.gov.uk/m/1", response)

    assert source == {
        "url": "https://ex.gov.uk/m/1",
        "etag": '"xyz"',
        "last_modified": "Wed, 19 Aug 2026 00:00:00 GMT",
    }
    assert scraper.index["meetings"]["https://ex.gov.uk/m/1"]["etag"] == '"xyz"'


def test_first_fetch_of_a_meeting_sends_no_conditional_headers():
    scraper = make_scraper()
    scraper.get_meeting_detail("2")

    url, headers = scraper.requested[0]
    assert "If-None-Match" not in headers
    assert "If-Modified-Since" not in headers


def test_a_known_meeting_is_fetched_conditionally():
    scraper = make_scraper()
    url = scraper.format_meeting_api_url("2")
    scraper.index["meetings"][url] = {
        "etag": '"abc"',
        "last_modified": "Wed, 19 Aug 2026 00:00:00 GMT",
    }

    scraper.get_meeting_detail("2")

    _, headers = scraper.requested[0]
    assert headers["If-None-Match"] == '"abc"'
    assert headers["If-Modified-Since"] == "Wed, 19 Aug 2026 00:00:00 GMT"


def test_304_means_the_stored_meeting_is_still_current():
    scraper = make_scraper()
    url = scraper.format_meeting_api_url("2")
    scraper.index["meetings"][url] = {"etag": '"abc"'}
    scraper.responses[url] = FakeResponse(status=304)

    with pytest.raises(MeetingNotModifiedException):
        scraper.get_meeting_detail("2")


def test_200_parses_the_detail_and_records_the_source():
    scraper = make_scraper()
    url = scraper.format_meeting_api_url("2")
    scraper.responses[url] = FakeResponse(
        content=b"<meeting><meetingid>2</meetingid></meeting>",
        headers={"etag": '"fresh"'},
    )

    detail = scraper.get_meeting_detail("2")

    assert detail.find("meetingid").text == "2"
    assert scraper._pending_source["etag"] == '"fresh"'
    assert scraper.index["meetings"][url]["etag"] == '"fresh"'


def test_a_server_with_no_validators_still_works():
    """Plenty of ModernGov installs send no ETag at all."""
    scraper = make_scraper()
    url = scraper.format_meeting_api_url("2")
    scraper.responses[url] = FakeResponse(content=b"<meeting/>", headers={})

    scraper.get_meeting_detail("2")

    assert scraper.index["meetings"][url] == {"etag": None, "last_modified": None}


def test_the_index_survives_a_round_trip_between_runs():
    """
    What run 1 wrote is what run 2 reads: this is the whole mechanism that
    makes skipping work across runs.
    """
    first = make_scraper()
    first.index["documents"]["https://ex.gov.uk/a.pdf"] = {
        "storage_key": "doc.pdf",
        "content_hash": "sha256:abc",
    }
    first.save_index()

    second = make_scraper(first.storage_session.files)
    assert second.index["documents"]["https://ex.gov.uk/a.pdf"]["storage_key"] == (
        "doc.pdf"
    )


def test_source_block_is_written_into_the_meeting_json():
    scraper = make_scraper()
    committee = BeautifulSoup(
        "<committee><committeeid>1</committeeid>"
        "<committeetitle>Audit Committee</committeetitle></committee>",
        features="xml",
    ).find("committee")
    detail = BeautifulSoup(
        "<meeting><meetingid>2</meetingid>"
        "<meetingdate>19/08/2026</meetingdate></meeting>",
        features="xml",
    ).find("meeting")
    scraper._pending_source = {"url": "https://ex.gov.uk/m/2", "etag": '"abc"'}

    meeting = scraper.get_single_meeting(committee, detail)

    assert meeting.as_dict()["source"]["etag"] == '"abc"'
