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
    # "CHAIR APPROVED Minutes Audit Committee 25 Nov 2025", on a meeting of
    # 10 February 2026: the previous meeting's, approved at this one. This
    # meeting's own minutes are the HTML in its agenda items.
    minutes_docs = [d for d in documents if d["category"] == "previous_minutes"]
    assert len(minutes_docs) == 1
    assert minutes_docs[0]["attachment_id"] == "60860"
    assert not [d for d in documents if d["category"] == "minutes"]
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


# --- the full record: a real Stroud Council meeting, trimmed ---


def stroud_meeting():
    scraper = make_scraper()
    scraper._pending_source = {
        "url": "https://stroud.example/GetMeeting?lMeetingId=1570"
    }
    detail = BeautifulSoup(load_xml("meeting_stroud_council.xml"), features="xml").find(
        "meeting"
    )
    committee = BeautifulSoup(
        "<committee><committeeid>143</committeeid><committeetitle>Council</committeetitle></committee>",
        features="xml",
    ).find("committee")
    return scraper.get_single_meeting(committee, detail)


def test_times_are_kept_as_given_including_past_midnight():
    meeting = stroud_meeting()

    assert meeting.times == {
        "scheduled": "7.00 pm",
        "actual_start": "7.00 pm",
        "actual_finish": "2.22 am",
        "planned_finish": "11.00 pm",
    }


def test_publication_flags_webcast_and_install():
    meeting = stroud_meeting()

    assert set(meeting.published) == {"agenda", "decisions", "minutes"}
    assert all(isinstance(v, bool) for v in meeting.published.values())
    assert meeting.is_webcast in (True, False)
    assert meeting.install_url == "https://democracy.example.gov.uk"


def test_agenda_items_in_order_with_minute_references_and_documents():
    items = stroud_meeting().agenda_items

    assert [(i["display_number"], i["title"]) for i in items] == [
        ("CL.054", "Apologies"),
        ("CL.056", "Minutes"),
        ("CL.059", items[2]["title"]),
    ]
    minutes_item = items[1]
    assert minutes_item["minutes_number"] == 56
    assert minutes_item["minutes_html"].lstrip().startswith("<div")
    assert minutes_item["document_ids"] == ["18071"]
    assert len(items[2]["document_ids"]) == 2
    assert {"is_decision", "is_call_in", "is_restricted", "declaration_count"} <= set(
        items[0]
    )


def test_documents_say_which_item_they_belong_to():
    meeting = stroud_meeting()
    by_id = {d["attachment_id"]: d for d in meeting.documents}
    minutes_item = meeting.agenda_items[1]

    previous = by_id["18071"]
    assert previous["agenda_item_id"] == minutes_item["id"]
    assert previous["owner_title"] == "Minutes"
    assert previous["publication_date"] == "2026-02-11"
    assert previous["is_decision_attachment"] is False


def test_the_previous_meetings_minutes_arent_this_meetings():
    """'Minutes , 18/12/2025 Council' on the 19 February 2026 meeting."""
    meeting = stroud_meeting()
    previous = next(d for d in meeting.documents if d["attachment_id"] == "18071")

    assert previous["category"] == "previous_minutes"


def test_categorise_by_the_date_in_the_title():
    scraper = make_scraper()
    assert (
        scraper.categorise_document("Minutes , 18/12/2025 Council", "2026-02-19")
        == "previous_minutes"
    )
    assert (
        scraper.categorise_document("Minutes of 19th February 2026", "2026-02-19")
        == "minutes"
    )
    assert (
        scraper.categorise_document("Printed minutes 19/02/2026 19:00", "2026-02-19")
        == "minutes"
    )
    assert (
        scraper.categorise_document("Minutes of the last meeting", "2026-02-19")
        == "minutes"
    )
    assert (
        scraper.categorise_document("Minutes, 18 December 2025", "2026-02-19")
        == "previous_minutes"
    )


def test_attendance_joins_to_councillors_by_member_id():
    attendance = stroud_meeting().attendance

    assert attendance[0] == {
        "member_id": "2566",
        "member_type": "U",
        "name": "Councillor Kate Kay",
        "role": "Chair",
        "attendance": "Present, as expected",
        "party": "GP",
        "ward": "N",
        "key_posts": "Chair of Council",
        "url": "https://Stroud.moderngov.co.uk/mgUserInfo.aspx?UID=2566",
    }
    assert [a["attendance"] for a in attendance].count("Apologies") == 2


def test_the_full_record_round_trips_through_storage():
    import json

    from lgsf.minutes.models import MeetingBase

    meeting = stroud_meeting()
    data = json.loads(meeting.as_json())

    class Session:
        def open(self, name):
            return json.dumps(data)

    again = MeetingBase.from_storage("x.json", Session())

    assert json.loads(again.as_json()) == data
