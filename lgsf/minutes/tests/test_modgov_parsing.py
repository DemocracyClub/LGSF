from pathlib import Path

from bs4 import BeautifulSoup

from lgsf.minutes.scrapers import ModGovMinutesScraper

FIXTURES = Path(__file__).parent / "fixtures"


class FakeStorageSession:
    """Minimal stand-in for a StorageSession, just captures writes."""

    def __init__(self):
        self.files = {}

    def write(self, filename, content):
        self.files[str(filename)] = content


def load_xml(name):
    return (FIXTURES / name).read_text()


def make_scraper():
    # Bypass ScraperBase.__init__ (which needs council metadata, a storage
    # backend, an HTTP client, etc.) so these tests exercise pure parsing
    # logic against fixture XML, with no network access.
    scraper = ModGovMinutesScraper.__new__(ModGovMinutesScraper)
    scraper.base_url = "https://democracy.example.gov.uk"
    scraper.meetings = set()
    scraper.new_data = True
    scraper.storage_session = FakeStorageSession()
    return scraper


def test_get_meetings_parses_committees():
    soup = BeautifulSoup(load_xml("committees.xml"), features="xml")
    committees = soup.find_all("committee")
    assert len(committees) == 3
    assert committees[-1].find("committeeid").text.strip() == "144"
    assert committees[-1].find("committeetitle").text.strip() == "Audit Committee"


def test_meeting_stub_ids_parsed():
    soup = BeautifulSoup(load_xml("meetings.xml"), features="xml")
    meetings = soup.find_all("meeting")
    ids = [m.find("meetingid").text.strip() for m in meetings]
    assert ids == ["4966", "4967"]


def test_parse_date():
    scraper = make_scraper()
    assert scraper.parse_date("10/02/2026") == "2026-02-10"


def test_categorise_document():
    scraper = make_scraper()
    assert scraper.categorise_document("Minutes of the last meeting") == "minutes"
    assert scraper.categorise_document("DRAFT AGENDA") == "agenda"
    assert scraper.categorise_document("Some other report") == "other"


def test_get_single_meeting_with_minutes():
    scraper = make_scraper()
    committee = BeautifulSoup(load_xml("committees.xml"), features="xml").find_all(
        "committee"
    )[-1]
    detail = BeautifulSoup(load_xml("meeting_with_minutes.xml"), features="xml").find(
        "meeting"
    )

    meeting = scraper.get_single_meeting(committee, detail)

    assert meeting.identifier == "4967"
    assert meeting.committee == "Audit Committee"
    assert meeting.committee_id == "144"
    assert meeting.date == "2026-02-10"
    assert meeting.status == "Confirmed"
    assert meeting.location == "Council Antechamber, Level 2, Town Hall Extension"
    assert meeting.minutes_published is True
    assert meeting.url == (
        "https://democracy.example.gov.uk/ieListDocuments.aspx?CId=144&MId=4967"
    )

    documents = meeting.documents
    assert len(documents) == 2
    minutes_docs = [d for d in documents if d["category"] == "minutes"]
    assert len(minutes_docs) == 1
    assert minutes_docs[0]["attachment_id"] == "60860"
    other_docs = [d for d in documents if d["category"] == "other"]
    assert len(other_docs) == 1

    # A minutes-bearing meeting should have its embedded HTML saved.
    assert any(name.endswith("-minutes.html") for name in scraper.storage_session.files)


def test_get_single_meeting_without_minutes():
    scraper = make_scraper()
    committee = BeautifulSoup(load_xml("committees.xml"), features="xml").find_all(
        "committee"
    )[-1]
    detail = BeautifulSoup(load_xml("meeting_no_minutes.xml"), features="xml").find(
        "meeting"
    )

    meeting = scraper.get_single_meeting(committee, detail)

    assert meeting.identifier == "4966"
    assert meeting.status == "Cancelled"
    assert meeting.minutes_published is False
    assert meeting.documents == []
    assert not any(
        name.endswith("-minutes.html") for name in scraper.storage_session.files
    )


def test_meeting_as_dict_shape():
    scraper = make_scraper()
    committee = BeautifulSoup(load_xml("committees.xml"), features="xml").find_all(
        "committee"
    )[-1]
    detail = BeautifulSoup(load_xml("meeting_with_minutes.xml"), features="xml").find(
        "meeting"
    )

    meeting = scraper.get_single_meeting(committee, detail)
    data = meeting.as_dict()

    assert data["raw_identifier"] == "4967"
    assert data["raw_committee"] == "Audit Committee"
    assert data["raw_committee_id"] == "144"
    assert data["minutes_published"] is True
    assert len(data["documents"]) == 2
