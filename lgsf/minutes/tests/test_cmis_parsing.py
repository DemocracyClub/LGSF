import datetime
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from lgsf.minutes.exceptions import MeetingNotModifiedException
from lgsf.minutes.scrapers import CMISMinutesScraper

FIXTURES = Path(__file__).parent / "fixtures"


class FakeStorageSession:
    def __init__(self):
        self.files = {}

    def write(self, filename, content):
        self.files[str(filename)] = content

    def write_bytes(self, filename, content):
        self.files[str(filename)] = content

    def open(self, filename, mode="r"):
        try:
            return self.files[str(filename)]
        except KeyError:
            raise FileNotFoundError(str(filename))


def load_html(name):
    return (FIXTURES / name).read_text()


class FakeConsole:
    def __init__(self):
        self.messages = []

    def log(self, message):
        self.messages.append(message)


def make_scraper():
    scraper = CMISMinutesScraper.__new__(CMISMinutesScraper)
    scraper.base_url = "https://birmingham.cmis.uk.com/birmingham/Meetings.aspx"
    scraper.meetings = set()
    scraper.new_data = True
    scraper.console = FakeConsole()
    scraper.storage_session = FakeStorageSession()
    return scraper


def test_get_module_id():
    scraper = make_scraper()
    text = load_html("cmis_base.html")
    match = scraper.MODULE_ID_PATTERN.search(text)
    assert match.group(1) == "397"


def test_format_list_view_url():
    scraper = make_scraper()
    url = scraper.format_list_view_url("397")
    assert url == (
        "https://birmingham.cmis.uk.com/birmingham/Meetings.aspx"
        "?ctl=MeetingCalendarListView&mid=397"
    )


def test_parse_row():
    scraper = make_scraper()
    soup = BeautifulSoup(load_html("cmis_listview.html"), "lxml")
    rows = soup.select("tr.rgRow, tr.rgAltRow")
    assert len(rows) == 2

    parsed = scraper.parse_row(rows[0])
    assert parsed["meeting_id"] == "15042"
    assert parsed["committee_id"] == "4"
    assert parsed["committee"] == "Planning Committee"
    assert parsed["date"] == datetime.date(2026, 7, 9)
    assert parsed["venue"] == "Committee Rooms 3 & 4"
    assert parsed["url"].endswith("/SelectedTab/Documents/Default.aspx")

    parsed2 = scraper.parse_row(rows[1])
    assert parsed2["committee"] == "Standards Committee"
    assert parsed2["date"] == datetime.date(2026, 7, 23)


def test_get_documents_with_minutes():
    scraper = make_scraper()
    soup = BeautifulSoup(load_html("cmis_detail_with_minutes.html"), "lxml")
    documents = scraper.get_documents(soup)

    assert len(documents) == 2
    minutes_docs = [d for d in documents if d["category"] == "minutes"]
    assert len(minutes_docs) == 1
    assert minutes_docs[0]["title"] == "Final Planning Minutes 16042026"
    assert minutes_docs[0]["attachment_id"] is None
    assert minutes_docs[0]["url"].startswith("https://birmingham.cmis.uk.com/")

    agenda_docs = [d for d in documents if d["category"] == "agenda"]
    assert len(agenda_docs) == 1


def test_get_documents_without_minutes():
    scraper = make_scraper()
    soup = BeautifulSoup(load_html("cmis_detail_no_minutes.html"), "lxml")
    documents = scraper.get_documents(soup)

    assert len(documents) == 1
    assert documents[0]["category"] == "agenda"
    assert not any(d["category"] == "minutes" for d in documents)


def test_get_single_meeting_with_minutes():
    scraper = make_scraper()
    row = scraper.parse_row(
        BeautifulSoup(load_html("cmis_listview.html"), "lxml").select("tr.rgRow")[0]
    )
    detail_soup = BeautifulSoup(load_html("cmis_detail_with_minutes.html"), "lxml")

    # This mirrors get_single_meeting's body to check the parsing pieces;
    # test_get_single_meeting_end_to_end drives the real method.
    meeting = scraper.add_meeting(
        row["url"],
        identifier=row["meeting_id"],
        title=f"{row['committee']} - {row['date'].isoformat()}",
        committee=row["committee"],
        date=row["date"].isoformat(),
        committee_id=row["committee_id"],
    )
    meeting.status = None
    meeting.location = row["venue"]
    meeting.documents = scraper.get_documents(detail_soup)
    meeting.minutes_published = any(
        d["category"] == "minutes" for d in meeting.documents
    )

    assert meeting.identifier == "15042"
    assert meeting.committee == "Planning Committee"
    assert meeting.committee_id == "4"
    assert meeting.date == "2026-07-09"
    assert meeting.location == "Committee Rooms 3 & 4"
    assert meeting.minutes_published is True
    assert len(meeting.documents) == 2


def make_fetching_scraper(detail_html, status=200, headers=None):
    """A CMIS scraper whose detail-page fetch is served from a fixture."""
    scraper = make_scraper()
    scraper.council_id = "BIR"
    scraper.options = {}
    scraper.extra_headers = {}
    scraper.unchanged_meetings = 0
    scraper.documents_downloaded = 0
    scraper.documents_skipped = 0
    scraper.requested = []

    class Response:
        def __init__(self):
            self.status_code = status
            self.headers = headers or {}
            self.text = detail_html

    def fake_get(url, extra_headers=None):
        scraper.requested.append((url, dict(extra_headers or {})))
        return Response()

    scraper.get = fake_get
    scraper.storage_session = FakeStorageSession()
    return scraper


def first_row(scraper):
    return scraper.parse_row(
        BeautifulSoup(load_html("cmis_listview.html"), "lxml").select("tr.rgRow")[0]
    )


def test_get_single_meeting_end_to_end():
    scraper = make_fetching_scraper(
        load_html("cmis_detail_with_minutes.html"), headers={"etag": '"cmis-1"'}
    )
    row = first_row(scraper)

    meeting, content = scraper.get_single_meeting(row)

    assert meeting.identifier == "15042"
    assert meeting.committee == "Planning Committee"
    assert meeting.date == "2026-07-09"
    assert meeting.location == "Committee Rooms 3 & 4"
    assert meeting.minutes_published is True
    assert len(meeting.documents) == 2
    assert meeting.source["etag"] == '"cmis-1"'
    assert content is not None


def test_get_single_meeting_sends_stored_validators():
    scraper = make_fetching_scraper(load_html("cmis_detail_with_minutes.html"))
    row = first_row(scraper)
    scraper.index["meetings"][row["url"]] = {"etag": '"cmis-1"'}

    scraper.get_single_meeting(row)

    _, headers = scraper.requested[0]
    assert headers["If-None-Match"] == '"cmis-1"'


def test_get_single_meeting_raises_on_304():
    scraper = make_fetching_scraper("", status=304)
    row = first_row(scraper)
    scraper.index["meetings"][row["url"]] = {"etag": '"cmis-1"'}

    with pytest.raises(MeetingNotModifiedException):
        scraper.get_single_meeting(row)


# --- month calendar paging ---------------------------------------------


def test_months_in_range_covers_every_month_spanned():
    scraper = make_scraper()
    months = list(
        scraper.months_in_range(datetime.date(2026, 5, 20), datetime.date(2026, 7, 3))
    )
    assert months == [
        datetime.date(2026, 5, 1),
        datetime.date(2026, 6, 1),
        datetime.date(2026, 7, 1),
    ]


def test_months_in_range_within_one_month():
    scraper = make_scraper()
    months = list(
        scraper.months_in_range(datetime.date(2026, 6, 2), datetime.date(2026, 6, 20))
    )
    assert months == [datetime.date(2026, 6, 1)]


def test_months_in_range_crosses_a_year_boundary():
    scraper = make_scraper()
    months = list(
        scraper.months_in_range(datetime.date(2025, 12, 15), datetime.date(2026, 1, 10))
    )
    assert months == [datetime.date(2025, 12, 1), datetime.date(2026, 1, 1)]


def test_calendar_url_template_built_from_a_meeting_link():
    """tabid and module id are both lifted from a real meeting link."""
    scraper = make_scraper()
    soup = BeautifulSoup(load_html("cmis_listview.html"), "lxml")

    template = scraper.get_calendar_url_template(soup)

    assert template is not None
    assert "/ctl/MeetingCalendarPublicNoJava/" in template
    assert "/tabid/" in template
    assert template.endswith("/Date/{date}/Default.aspx")
    assert "ViewMeetingPublic" not in template


def test_calendar_url_template_is_none_without_a_usable_link():
    scraper = make_scraper()
    soup = BeautifulSoup("<html><body><a href='/about'>About</a></body></html>", "lxml")
    assert scraper.get_calendar_url_template(soup) is None


def test_parse_calendar_page_reads_a_whole_month():
    scraper = make_scraper()
    rows = scraper.parse_calendar_page(
        load_html("cmis_calendar_month.html"), datetime.date(2026, 6, 1)
    )

    assert [row["date"] for row in rows] == [
        datetime.date(2026, 6, 8),
        datetime.date(2026, 6, 9),
        datetime.date(2026, 6, 9),
        datetime.date(2026, 6, 30),
    ]
    assert rows[0]["meeting_id"] == "15028"
    assert rows[0]["committee_id"] == "6"
    assert rows[0]["committee"] == "Licensing Sub-Committee A"


def test_parse_calendar_page_handles_several_meetings_in_one_day():
    scraper = make_scraper()
    rows = scraper.parse_calendar_page(
        load_html("cmis_calendar_month.html"), datetime.date(2026, 6, 1)
    )
    ninth = [r for r in rows if r["date"] == datetime.date(2026, 6, 9)]
    assert [r["meeting_id"] for r in ninth] == ["15029", "15030"]


def test_parse_calendar_page_takes_the_year_from_the_month_requested():
    """The day cells say "Monday, 8 June" with no year."""
    scraper = make_scraper()
    rows = scraper.parse_calendar_page(
        load_html("cmis_calendar_month.html"), datetime.date(2027, 6, 1)
    )
    assert all(row["date"].year == 2027 for row in rows)


def test_parse_calendar_page_skips_days_with_no_meetings():
    scraper = make_scraper()
    rows = scraper.parse_calendar_page(
        load_html("cmis_calendar_month.html"), datetime.date(2026, 6, 1)
    )
    assert datetime.date(2026, 6, 1) not in [row["date"] for row in rows]


# --- meeting summary ---------------------------------------------------


def test_parse_meeting_summary_reads_labelled_fields():
    scraper = make_scraper()
    soup = BeautifulSoup(load_html("cmis_meeting_summary.html"), "lxml")

    summary = scraper.parse_meeting_summary(soup)

    assert summary["Status"] == "Scheduled"
    assert summary["Venue"] == "On-line meeting"
    assert summary["Committee"] == "Licensing Sub-Committee A"


def test_meeting_summary_supplies_venue_and_status():
    """
    The month calendar carries neither, so both come from the meeting
    page.
    """
    scraper = make_fetching_scraper(load_html("cmis_meeting_summary.html"))
    row = {
        "url": "https://birmingham.cmis.uk.com/birmingham/Meetings/tabid/70"
        "/ctl/ViewMeetingPublic/mid/397/Meeting/15218/Committee/6/Default.aspx",
        "meeting_id": "15218",
        "committee_id": "6",
        "committee": "Licensing Sub-Committee A",
        "date": datetime.date(2026, 8, 3),
        "venue": None,
    }

    meeting, _ = scraper.get_single_meeting(row)

    assert meeting.status == "Scheduled"
    assert meeting.location == "On-line meeting"


# --- get_meetings orchestration ----------------------------------------


def make_paging_scraper(pages, weeks_back=4, weeks_forward=1):
    """A scraper whose every GET is served from a {url_fragment: html} dict."""
    scraper = make_scraper()
    scraper.council_id = "BIR"
    scraper.options = {}
    scraper.extra_headers = {}
    scraper.weeks_back = weeks_back
    scraper.weeks_forward = weeks_forward
    scraper.fetched = []

    # Most specific first: every one of these URLs also contains the base
    # URL, so plain substring matching in dict order picks the wrong page.
    order = ["MeetingCalendarPublicNoJava", "MeetingCalendarListView"]

    def fake_get_text(url, extra_headers=None):
        scraper.fetched.append(url)
        for fragment in order:
            if fragment in url and fragment in pages:
                return pages[fragment]
        return pages.get("base", load_html("cmis_base.html"))

    scraper.get_text = fake_get_text
    return scraper


def test_get_meetings_fetches_one_page_per_month_in_the_window():
    scraper = make_paging_scraper(
        {
            "MeetingCalendarListView": load_html("cmis_listview.html"),
            "MeetingCalendarPublicNoJava": load_html("cmis_calendar_month.html"),
        }
    )
    start, end = scraper.date_range
    expected_months = len(list(scraper.months_in_range(start, end)))

    scraper.get_meetings()

    calendar_requests = [u for u in scraper.fetched if "NoJava" in u]
    assert len(calendar_requests) == expected_months
    assert all("/Date/" in u for u in calendar_requests)


def test_get_meetings_falls_back_to_the_list_view():
    """An install with no usable meeting link still returns what it can."""
    scraper = make_paging_scraper({"MeetingCalendarListView": "<html></html>"})

    scraper.get_meetings()

    assert not [u for u in scraper.fetched if "NoJava" in u]
    assert any("Failed" not in m for m in scraper.console.messages)


def test_get_meetings_filters_to_the_scrape_window():
    """June 2026 meetings are far outside a window around today."""
    scraper = make_paging_scraper(
        {
            "MeetingCalendarListView": load_html("cmis_listview.html"),
            "MeetingCalendarPublicNoJava": load_html("cmis_calendar_month.html"),
        }
    )
    assert scraper.get_meetings() == []


def test_get_meetings_deduplicates_by_meeting_id():
    """A meeting appearing on two month pages is only scraped once."""
    scraper = make_paging_scraper(
        {
            "MeetingCalendarListView": load_html("cmis_listview.html"),
            "MeetingCalendarPublicNoJava": load_html("cmis_calendar_month.html"),
        }
    )
    today = datetime.date.today()
    rows = scraper.parse_calendar_page(
        load_html("cmis_calendar_month.html"), datetime.date(2026, 6, 1)
    )
    for row in rows:
        row["date"] = today

    scraper.parse_calendar_page = lambda text, month: [dict(r) for r in rows]

    meetings = scraper.get_meetings()

    assert [m["meeting_id"] for m in meetings] == ["15028", "15029", "15030", "15099"]


@pytest.mark.parametrize(
    "module_name",
    ["MeetingCalendarPublic", "MeetingCalendarPublicList"],
)
def test_module_id_found_whatever_the_module_is_suffixed_with(module_name):
    """
    Installs vary: Nottinghamshire's module is MeetingCalendarPublicList,
    which an exact-name match misses entirely.
    """
    scraper = make_scraper()
    html = f'<input name="dnn$ctr397${module_name}$hidden" />'
    scraper.get_text = lambda url, extra_headers=None: html

    assert scraper.get_module_id() == "397"


def test_module_id_missing_raises_a_useful_error():
    scraper = make_scraper()
    scraper.get_text = lambda url, extra_headers=None: "<html></html>"

    with pytest.raises(ValueError, match="Could not find CMIS meetings module id"):
        scraper.get_module_id()
