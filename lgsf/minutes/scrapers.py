import abc
import datetime
import json
import re
from functools import cached_property
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup, Tag

from lgsf.aws_lambda.run_log import RunLog
from lgsf.minutes import MeetingBase
from lgsf.minutes.exceptions import (
    MeetingNotModifiedException,
    SkipMeetingException,
)
from lgsf.scrapers import ScraperBase
from lgsf.scrapers.documents import DocumentSavingMixin
from lgsf.storage.backends.base import StorageMode


_DATE_PATTERNS = (
    (re.compile(r"\b(\d{1,2})[/.](\d{1,2})[/.](\d{4})\b"), "dmy"),
    (re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})\b"), "d month y"),
)


def dates_in(text):
    """The ISO dates written in a title, as 18/12/2025 or 18 December 2025."""
    found = set()
    for pattern, kind in _DATE_PATTERNS:
        for match in pattern.finditer(text or ""):
            try:
                if kind == "dmy":
                    day, month, year = (int(g) for g in match.groups())
                    found.add(datetime.date(year, month, day).isoformat())
                else:
                    day, month, year = match.groups()
                    found.add(
                        datetime.datetime.strptime(
                            f"{int(day)} {month[:3]} {year}", "%d %b %Y"
                        )
                        .date()
                        .isoformat()
                    )
            except ValueError:
                continue
    return found


class BaseMinutesScraper(DocumentSavingMixin, ScraperBase):
    tags = []
    class_tags = []
    ext = "html"
    scraper_object_type = "Minutes"
    service_name = "minutes"

    # Council-run ModernGov installs are far more variable than the
    # vendor-hosted ones: moderngov.co.uk sites answer well inside a
    # second, while self-hosted ones can take up to a minute. The
    # framework default of 30s is too short for the slowest of them.
    timeout = 60

    # Minutes are an append-only historical record: a meeting that has
    # dropped out of the rolling scrape window is still a fact, so runs
    # must add to what's stored rather than replacing it.
    storage_mode = StorageMode.ACCUMULATE

    # Meetings are scraped over a rolling window around today
    weeks_back = 4
    weeks_forward = 1

    # When True, every document linked from a meeting is downloaded to the
    # document store. The full agenda pack is kept, not just the minutes:
    # categorisation labels documents, it doesn't filter them.
    save_documents = True

    # Commit progress every this many meetings saved, where the storage
    # backend can do that cheaply. A document is stored under a key made
    # from its bytes, so one stored but not yet named in a saved index is
    # downloaded again by the next run: checkpointing keeps that to the
    # documents of the last few meetings, not the whole run.
    checkpoint_every = 50

    #: Where the run index lives inside the metadata store.
    INDEX_FILE_NAME = "_index.json"
    INDEX_VERSION = 1

    def __init__(self, options, console):
        super().__init__(options, console)
        self.meetings = set()
        self.new_data = True
        self.unchanged_meetings = 0
        self.meetings_saved = 0

    @property
    def skip_documents(self):
        """
        True when this run should find documents but not download them.

        Set by --skip-documents. A full run of every council downloads
        tens of gigabytes, which is a lot of transfer to answer "does this
        scraper still work?", so a sweep can leave the files alone and
        still check that meetings and their documents are found.
        """
        return bool(self.options.get("skip_documents"))

    @cached_property
    def index(self):
        """
        What previous runs already fetched, keyed by URL.

        The storage session API can read and write files but not list them,
        so rather than trying to enumerate hundreds of stored meeting JSON
        files we keep one index alongside them. It records the HTTP
        validators for each meeting page and, for each document, where it
        was stored and what its hash was.

        This is a cache, not a source of truth: every entry is verified
        against the document store before it's used to skip work, so a
        stale or hand-deleted index costs a re-download rather than losing
        a document.
        """
        empty = {"version": self.INDEX_VERSION, "meetings": {}, "documents": {}}
        try:
            stored = json.loads(
                self.storage_session.open(self._file_name(self.INDEX_FILE_NAME))
            )
        except (FileNotFoundError, ValueError):
            return empty

        if stored.get("version") != self.INDEX_VERSION:
            # Index written by a different version of this scraper; start
            # again rather than guessing at its shape.
            return empty

        empty.update(
            {
                "meetings": stored.get("meetings") or {},
                "documents": stored.get("documents") or {},
            }
        )
        return empty

    def save_index(self):
        """Write the run index back out for the next run to read."""
        self.index["updated"] = datetime.datetime.now().isoformat()
        self.storage_session.write(
            self._file_name(self.INDEX_FILE_NAME),
            json.dumps(self.index, indent=4, sort_keys=True),
        )

    def validators_for(self, url):
        """Return (etag, last_modified) recorded for ``url``, if any."""
        entry = self.index["meetings"].get(url) or {}
        return entry.get("etag"), entry.get("last_modified")

    def record_source(self, url, response):
        """
        Note the validators a response came back with, and return the
        ``source`` block stored on the meeting.
        """
        source = {
            "url": url,
            "etag": self.response_header(response, "etag"),
            "last_modified": self.response_header(response, "last-modified"),
        }
        self.index["meetings"][url] = {
            "etag": source["etag"],
            "last_modified": source["last_modified"],
        }
        return source

    @property
    def date_range(self):
        """
        The meetings to look at: weeks_back before today to weeks_forward
        after, or for a backfill from the council's date in --since-file,
        else from --since. A backfill is resumable: a
        meeting whose detail is unchanged since it was stored is answered
        with a 304 where the council supports it, and not saved again.
        """
        today = datetime.date.today()
        since = (self.options.get("since_file") or {}).get(
            str(self.options.get("council", "")).upper()
        ) or self.options.get("since")
        if since:
            start = datetime.date.fromisoformat(since)
        else:
            start = today - datetime.timedelta(weeks=self.weeks_back)
        end = today + datetime.timedelta(weeks=self.weeks_forward)
        return start, end

    @abc.abstractmethod
    def get_meetings(self):
        pass

    @abc.abstractmethod
    def get_single_meeting(self, meeting_data):
        pass

    def add_meeting(
        self,
        url,
        identifier: str,
        title: str,
        committee: str,
        date: str,
        committee_id: str = None,
    ):
        assert committee, f"No Committee for {url}"
        assert title, f"No Title for {url}"
        assert date, f"No Date for {url}"
        assert identifier, f"No Identifier for {url}"
        meeting = MeetingBase(
            url,
            identifier=identifier,
            title=title,
            committee=committee,
            date=date,
            committee_id=committee_id,
        )
        self.meetings.add(meeting)
        return meeting

    @property
    def get_tags(self):
        return self.tags + self.class_tags

    @property
    def report_items(self):
        return list(self.meetings)

    def run(self, run_log: RunLog):
        for meeting_data in self.get_meetings():
            try:
                meeting = self.get_single_meeting(meeting_data)
                self.process_meeting(meeting, meeting_data)
            except MeetingNotModifiedException:
                self.unchanged_meetings += 1
                continue
            except SkipMeetingException:
                continue

        self.save_index()

        # Finalize storage with run log data
        self.finalize_storage(run_log)

        self.report()

    def prettify_meeting_str(self, meeting_raw_str):
        if isinstance(meeting_raw_str, dict):
            return json.dumps(meeting_raw_str, indent=4)
        if isinstance(meeting_raw_str, Tag):
            return meeting_raw_str.prettify()
        return None

    def process_meeting(self, meeting, meeting_raw_str):
        formatted_meeting_raw_str = self.prettify_meeting_str(meeting_raw_str)

        self.documents_linked += len(getattr(meeting, "documents", []) or [])

        # Documents first: storing them adds the hash and storage key to each
        # document dict, and the meeting JSON is serialised from those, so
        # saving the meeting first would record documents with no location.
        # Unlike decisions, a meeting isn't marked incomplete when one of
        # its documents fails, so how many did isn't needed here.
        if self.save_documents and not self.skip_documents:
            self.save_record_documents(meeting)

        self.save_meeting(formatted_meeting_raw_str, meeting)

        # Here rather than in run(), because ModernGov and CMIS each have
        # their own run loop, and all three come through here.
        self.meetings_saved += 1
        if self.checkpoint_every and self.meetings_saved % self.checkpoint_every == 0:
            self.checkpoint()

    def save_meeting(self, raw_content, meeting_obj):
        assert type(meeting_obj) is MeetingBase, "Scrapers must return a meeting object"
        file_name = "{}.{}".format(meeting_obj.as_file_name(), self.ext)
        self.save_raw(file_name, raw_content)
        self.save_json(meeting_obj)

    def report(self):
        # Unlike councillors, zero meetings in the scrape window is
        # legitimate, so don't treat a low count as an error.
        self.console.log(
            f"Found {len(self.meetings)} meetings "
            f"({self.unchanged_meetings} unchanged since last run)"
        )
        if self.skip_documents:
            self.console.log(
                f"Documents: {self.documents_linked} found, none downloaded "
                "(--skip-documents)"
            )
        else:
            self.console.log(
                f"Documents: {self.documents_downloaded} downloaded "
                f"({self.documents_already_in_store} of them already in the "
                f"store, so not written again), "
                f"{self.documents_skipped} already stored "
                f"({self.documents_from_other_types} of them by another data type), "
                f"{self.documents_unavailable} unavailable (404, 410 or empty)"
            )
            if self.documents_failed:
                self.console.log(
                    f"[yellow]{self.documents_failed} documents failed to "
                    f"download[/yellow]"
                )

    def categorise_document(self, title, meeting_date=None):
        """
        "minutes", "previous_minutes", "agenda" or "other", from a title.

        A meeting's papers usually include the minutes of the meeting before
        it, for approval ("Minutes , 18/12/2025 Council" on a 19 February
        meeting). A minutes document whose title names a date other than
        the meeting's is "previous_minutes", not this meeting's minutes.
        """
        lowered = title.lower()
        if "minute" in lowered:
            named = dates_in(title)
            if meeting_date and named and meeting_date not in named:
                return "previous_minutes"
            return "minutes"
        if "agenda" in lowered:
            return "agenda"
        return "other"


class CustomHTMLMinutesScraper(BaseMinutesScraper):
    """
    Base for councils whose meetings are on neither ModernGov nor CMIS.

    Supplies page fetching and parsing. `get_meetings` and `get_single_meeting`
    are still abstract, and are what the council's scraper implements.
    """

    class_tags = ["html"]

    def get_page(self, url):
        """The URL fetched and parsed, ready to select against."""
        return BeautifulSoup(
            self.get_text(url, extra_headers=self.extra_headers), "lxml"
        )


class ModGovMinutesScraper(BaseMinutesScraper):
    class_tags = ["modgov"]
    ext = "xml"

    DATE_FORMAT = "%d/%m/%Y"

    def format_committees_api_url(self):
        return "{}/mgWebService.asmx/GetCommittees".format(self.base_url)

    def format_meetings_api_url(self, committee_id, start, end):
        return (
            "{}/mgWebService.asmx/GetMeetings?lCommitteeId={}"
            "&sFromDate={}&sToDate={}".format(
                self.base_url,
                committee_id,
                start.strftime(self.DATE_FORMAT),
                end.strftime(self.DATE_FORMAT),
            )
        )

    def format_meeting_api_url(self, meeting_id):
        return "{}/mgWebService.asmx/GetMeeting?lMeetingId={}".format(
            self.base_url, meeting_id
        )

    def run(self, run_log: RunLog):
        """
        ModernGov needs a three-level fetch (committees -> meeting stubs for
        each committee's date window -> full detail per meeting), so - like
        ModGovCouncillorScraper - this overrides the base run() loop rather
        than using the generic get_meetings()/get_single_meeting() pairing.
        """
        committees = self.get_meetings()
        self.save_committees(committees)
        for committee in committees:
            committee_id = committee.find("committeeid").text.strip()
            for meeting_stub in self.get_meeting_stubs(committee_id):
                meeting_id_tag = meeting_stub.find("meetingid")
                if not meeting_id_tag or not meeting_id_tag.text.strip():
                    continue
                try:
                    detail = self.get_meeting_detail(meeting_id_tag.text.strip())
                except MeetingNotModifiedException:
                    self.unchanged_meetings += 1
                    continue
                if detail is None:
                    continue
                try:
                    meeting = self.get_single_meeting(committee, detail)
                    self.process_meeting(meeting, detail)
                except SkipMeetingException:
                    continue

        self.save_index()
        self.finalize_storage(run_log)
        self.report()

    def committee_record(self, committee):
        """
        One committee as its own record. ModernGov lists expired and
        deleted committees too; a renamed committee is a new one, so the
        old name is kept as an expired record (Manchester has 101, 72 of
        them expired).
        """
        committee_id = self.tag_text(committee, "committeeid")
        return {
            "committee_id": committee_id,
            "title": self.tag_text(committee, "committeetitle"),
            "is_expired": self.tag_bool(committee, "committeeexpired"),
            "is_deleted": self.tag_bool(committee, "committeedeleted"),
            "category": self.tag_text(committee, "committeecategory"),
            "url": f"{self.base_url}/mgCommitteeDetails.aspx?ID={committee_id}",
            "install_url": self.base_url,
            # Everything ModernGov said, for fields not read above
            "raw": {
                child.name: child.get_text(strip=True) or None
                for child in committee.find_all(recursive=False)
            },
        }

    def save_committees(self, committees):
        """committees/<id>.json beside the meetings, one per committee."""
        for committee in committees:
            record = self.committee_record(committee)
            if not record["committee_id"]:
                continue
            self._save_file(
                "committees",
                f"{record['committee_id']}.json",
                json.dumps(record, indent=4, sort_keys=True),
            )

    def get_meetings(self):
        text = self.get_text(
            self.format_committees_api_url(), extra_headers=self.extra_headers
        )
        soup = BeautifulSoup(text, features="xml")
        return soup.find_all("committee")

    def get_meeting_stubs(self, committee_id):
        start, end = self.date_range
        text = self.get_text(
            self.format_meetings_api_url(committee_id, start, end),
            extra_headers=self.extra_headers,
        )
        soup = BeautifulSoup(text, features="xml")
        return soup.find_all("meeting")

    def get_meeting_detail(self, meeting_id):
        """
        Fetch one meeting's full detail, asking the server first whether
        anything has changed since we last looked.

        Raises MeetingNotModifiedException on a 304 so the caller can leave
        the stored copy alone. ModernGov installs vary in whether they send
        validators at all; where they don't, this is an ordinary GET.
        """
        url = self.format_meeting_api_url(meeting_id)
        etag, last_modified = self.validators_for(url)

        response = self.get_conditional(
            url,
            etag=etag,
            last_modified=last_modified,
            extra_headers=self.extra_headers,
        )
        if self.response_status(response) == 304:
            raise MeetingNotModifiedException(url)

        self._pending_source = self.record_source(url, response)

        text = response.text
        if callable(text):
            text = text()
        soup = BeautifulSoup(text, features="xml")
        return soup.find("meeting")

    def get_single_meeting(self, committee, detail):
        committee_id = committee.find("committeeid").text.strip()
        committee_title = committee.find("committeetitle").text.strip()

        identifier = detail.find("meetingid").text.strip()
        date = self.parse_date(detail.find("meetingdate").text.strip())
        url = "{}/ieListDocuments.aspx?CId={}&MId={}".format(
            self.base_url, committee_id, identifier
        )

        meeting = self.add_meeting(
            url,
            identifier=identifier,
            title=f"{committee_title} - {date}",
            committee=committee_title,
            date=date,
            committee_id=committee_id,
        )

        status_tag = detail.find("meetingstatus")
        meeting.status = status_tag.text.strip() if status_tag else None
        location_tag = detail.find("meetinglocation")
        meeting.location = location_tag.text.strip() if location_tag else None
        minutes_published_tag = detail.find("minutepublished")
        meeting.minutes_published = bool(
            minutes_published_tag and minutes_published_tag.text.strip() == "True"
        )
        meeting.source = getattr(self, "_pending_source", {})
        meeting.documents = self.get_documents(detail, meeting_date=date)
        meeting.times = {
            "scheduled": self.tag_text(detail, "meetingtime"),
            "actual_start": self.tag_text(detail, "meetingactualstarttime"),
            "actual_finish": self.tag_text(detail, "meetingactualfinishtime"),
            "planned_finish": self.tag_text(detail, "meetingplannedfinishtime"),
        }
        meeting.published = {
            "agenda": self.tag_bool(detail, "agendapublished"),
            "decisions": self.tag_bool(detail, "decisionpublished"),
            "minutes": self.tag_bool(detail, "minutepublished"),
        }
        meeting.is_webcast = self.tag_bool(detail, "iswebcast")
        meeting.agenda_items = self.get_agenda_items(detail)
        meeting.attendance = self.get_attendance(detail)
        meeting.install_url = self.base_url

        self.save_minutes_html(meeting, detail)

        if self.exclude_meeting_hook(meeting):
            raise SkipMeetingException

        return meeting

    def parse_date(self, date_str):
        """ModernGov dates are dd/mm/yyyy; store ISO format."""
        return datetime.datetime.strptime(date_str, self.DATE_FORMAT).date().isoformat()

    @staticmethod
    def tag_text(element, name):
        """A child element's text, stripped, or None if absent or empty."""
        tag = element.find(name, recursive=False) if element else None
        if tag is None:
            return None
        return tag.text.strip() or None

    @classmethod
    def tag_bool(cls, element, name):
        """A ModernGov "True"/"False" child as a bool, or None."""
        value = cls.tag_text(element, name)
        return None if value is None else value == "True"

    @classmethod
    def tag_int(cls, element, name):
        value = cls.tag_text(element, name)
        try:
            return int(value) if value is not None else None
        except ValueError:
            return None

    #: Some installs answer only on https but give their document links as
    #: http://, so every download waits out a connect timeout and a run with
    #: many documents is cut off. Set this to give links on the install's
    #: own host the scheme of base_url. Off by default: the index is keyed
    #: by URL, so changing a link fetches that document again.
    documents_use_base_url_scheme = False

    def document_url(self, url):
        if not (url and self.documents_use_base_url_scheme):
            return url
        base, link = urlsplit(self.base_url), urlsplit(url)
        if link.hostname == base.hostname and link.scheme != base.scheme:
            return link._replace(scheme=base.scheme).geturl()
        return url

    def get_documents(self, detail, meeting_date=None):
        """
        The meeting's documents, each once, with the agenda item it is
        attached to and what ModernGov says about it.
        """
        documents = []
        seen = set()
        for linked_doc in detail.find_all("linkeddoc"):
            url = self.document_url(self.tag_text(linked_doc, "url"))
            if not url or url in seen:
                continue
            seen.add(url)
            title = self.tag_text(linked_doc, "title") or ""
            item = linked_doc.find_parent("agendaitem")
            documents.append(
                {
                    "title": title,
                    "url": url,
                    "attachment_id": self.tag_text(linked_doc, "attachmentid"),
                    "category": self.categorise_document(title, meeting_date),
                    "agenda_item_id": self.tag_text(item, "agendaitemid")
                    if item
                    else None,
                    "owner_title": self.tag_text(linked_doc, "ownertitle"),
                    "publication_date": self.parse_optional_date(
                        self.tag_text(linked_doc, "publicationdate")
                    ),
                    "is_restricted": self.tag_bool(linked_doc, "isrestricted"),
                    "is_decision_attachment": self.tag_bool(
                        linked_doc, "isdecisionattachment"
                    ),
                }
            )
        return documents

    def parse_optional_date(self, value):
        try:
            return self.parse_date(value) if value else None
        except ValueError:
            return value

    def get_agenda_items(self, detail):
        """Each agenda item in ModernGov's order, with the ids of its documents."""
        items = []
        for item in detail.find_all("agendaitem"):
            items.append(
                {
                    "id": self.tag_text(item, "agendaitemid"),
                    "number": self.tag_int(item, "agendaitemnumber"),
                    "sub_number": self.tag_text(item, "agendaitemsubnumber"),
                    # The minute reference, e.g. Stroud's "CL.025a"
                    "display_number": self.tag_text(item, "fulldisplaynumberformat"),
                    "minutes_number": self.tag_int(item, "minutesitemnumber"),
                    "minutes_sub_number": self.tag_text(item, "minutesitemsubnumber"),
                    "title": self.tag_text(item, "agendaitemtitle"),
                    "agenda_html": self.tag_text(item, "agendatext"),
                    "minutes_html": self.tag_text(item, "minutesnonemptyhtmlbody"),
                    "is_decision": self.tag_bool(item, "isdecision"),
                    "is_decision_published": self.tag_bool(item, "isdecisionpublished"),
                    "is_minute_published": self.tag_bool(item, "isminutepublished"),
                    "is_call_in": self.tag_bool(item, "iscallin"),
                    "is_restricted": self.tag_bool(item, "isrestricted"),
                    "is_public_can_speak": self.tag_bool(item, "ispubliccanspeak"),
                    "declaration_count": self.tag_int(item, "declarationcount"),
                    "refer_to_committee": self.tag_text(item, "refertocommittee"),
                    "preferred_date": self.tag_text(item, "preferreddate"),
                    "document_ids": [
                        self.tag_text(doc, "attachmentid")
                        for doc in item.find_all("linkeddoc")
                        if self.tag_text(doc, "attachmentid")
                    ],
                }
            )
        return items

    def get_attendance(self, detail):
        """
        Everyone ModernGov lists against the meeting, as it gives them.
        member_id is the id the ModernGov councillors scraper uses, so
        attendance joins to councillors.
        """
        attendance = []
        attendees = detail.find("attendees")
        for attendee in attendees.find_all("attendee") if attendees else []:
            attendance.append(
                {
                    "member_id": attendee.get("memberid") or None,
                    "member_type": attendee.get("membertype") or None,
                    "name": attendee.get("name") or None,
                    "role": attendee.get("roledescription") or None,
                    "attendance": attendee.get("attendance") or None,
                    "party": attendee.get("politicalparty") or None,
                    "ward": attendee.get("ward") or None,
                    "key_posts": attendee.get("keyposts") or None,
                    "url": self.tag_text(attendee, "url"),
                    "reason": None,
                    "substituted_by": None,
                }
            )
        return attendance

    def save_minutes_html(self, meeting, detail):
        """
        ModernGov embeds the text of published minutes in each agenda item
        (as escaped HTML in <minutesnonemptyhtmlbody>). Save the combined
        HTML alongside the raw XML.
        """
        parts = []
        for item in detail.find_all("agendaitem"):
            body = item.find("minutesnonemptyhtmlbody")
            if body and body.text.strip():
                title_tag = item.find("agendaitemtitle")
                if title_tag and title_tag.text.strip():
                    parts.append(f"<h2>{title_tag.text.strip()}</h2>")
                parts.append(body.text)
        if parts:
            file_name = f"{meeting.as_file_name()}-minutes.html"
            self.save_raw(file_name, "\n".join(parts))

    def exclude_meeting_hook(self, meeting: MeetingBase):
        return False


class CMISMinutesScraper(BaseMinutesScraper):
    """
    CMIS councils publish a "Meetings" calendar module (a DNN/Telerik web
    part). Its default view is a JS calendar widget, but the same module
    also serves plain server-rendered views that need no session, cookies
    or JS.

    Two of those views matter here:

    ``ctl=MeetingCalendarListView`` renders a table of meetings, but only
    ever for the server's *current* calendar month - it ignores any date
    parameter, so it can't reach the rest of a scrape window.

    ``ctl/MeetingCalendarPublicNoJava/mid/<id>/Date/<yyyy-mm-dd>/`` renders
    a whole month's calendar for *any* date, as a plain GET. That's what
    this scraper walks, one request per month spanned by the window, which
    is what lets CMIS councils return the same rolling window as ModernGov.

    It has to be reached in DNN's path form rather than as a query string,
    and that form needs the site's ``tabid`` as well as the module id.
    Neither is published anywhere convenient, so both are recovered from a
    real meeting link on the list view - see ``get_calendar_url_template``.

    The list view remains the fallback for installs where no such link can
    be found (typically a month with no meetings at all).
    """

    class_tags = ["cmis"]
    ext = "html"

    # Rendering a month of the no-JS calendar is slow - Birmingham takes
    # around 40 seconds - and comfortably exceeds the framework default.
    timeout = 120

    # The module is named MeetingCalendarPublic on most installs but
    # carries a suffix on some (Nottinghamshire: MeetingCalendarPublicList),
    # so match the stem rather than the whole name.
    MODULE_ID_PATTERN = re.compile(r"dnn\$ctr(\d+)\$MeetingCalendarPublic\w*\$")
    ROW_LINK_TEXT_PATTERN = re.compile(
        r"^(?P<date>\d{1,2} \w+ \d{4} \d{2}:\d{2}) - (?P<committee>.+)$"
    )
    ROW_URL_PATTERN = re.compile(
        r"/Meeting/(?P<meeting_id>\d+)/Committee/(?P<committee_id>\d+)/"
    )
    LINK_TEXT_DATE_FORMAT = "%d %b %Y %H:%M"

    # A meeting link in DNN path form, e.g.
    # .../Meetings/tabid/70/ctl/ViewMeetingPublic/mid/397/Meeting/15218/...
    # Everything up to and including the module id is reusable as the
    # prefix for any other view of the same module.
    CALENDAR_LINK_PATTERN = re.compile(
        r"(?P<prefix>\S*/tabid/\d+)/ctl/\w+/(?P<mid>mid/\d+)/", re.IGNORECASE
    )

    # A day heading in the month calendar, e.g. "Monday, 8 June". The year
    # is not repeated in the cell, so it comes from the requested month.
    CALENDAR_DAY_PATTERN = re.compile(
        r"^\w+day,\s+(?P<day>\d{1,2})\s+(?P<month>\w+)(?:\s+(?P<year>\d{4}))?$"
    )

    # A meeting entry within a day, e.g. "10:00 Licensing Sub-Committee A".
    CALENDAR_ENTRY_PATTERN = re.compile(
        r"^(?:(?P<time>\d{1,2}:\d{2})\s+)?(?P<committee>.+)$"
    )

    def run(self, run_log: RunLog):
        for row in self.get_meetings():
            try:
                meeting, raw_content = self.get_single_meeting(row)
            except MeetingNotModifiedException:
                self.unchanged_meetings += 1
                continue
            except SkipMeetingException:
                continue

            self.process_meeting(meeting, raw_content)

        self.save_index()
        self.finalize_storage(run_log)
        self.report()

    def get_module_id(self):
        text = self.get_text(self.base_url, extra_headers=self.extra_headers)
        match = self.MODULE_ID_PATTERN.search(text)
        if not match:
            raise ValueError(
                f"Could not find CMIS meetings module id at {self.base_url}"
            )
        return match.group(1)

    def format_list_view_url(self, module_id):
        separator = "&" if "?" in self.base_url else "?"
        return f"{self.base_url}{separator}ctl=MeetingCalendarListView&mid={module_id}"

    def parse_row(self, row):
        cells = row.find_all("td")
        if len(cells) < 2:
            return None
        link = cells[0].find("a")
        if not link or not link.get("href"):
            return None

        text_match = self.ROW_LINK_TEXT_PATTERN.match(link.get_text(strip=True))
        url_match = self.ROW_URL_PATTERN.search(link["href"])
        if not text_match or not url_match:
            return None

        try:
            date = datetime.datetime.strptime(
                text_match.group("date"), self.LINK_TEXT_DATE_FORMAT
            ).date()
        except ValueError:
            return None

        return {
            "url": link["href"],
            "meeting_id": url_match.group("meeting_id"),
            "committee_id": url_match.group("committee_id"),
            "committee": text_match.group("committee").strip(),
            "date": date,
            "venue": cells[1].get_text(strip=True),
        }

    def months_in_range(self, start, end):
        """Yield the first of each calendar month spanned by start..end."""
        month = start.replace(day=1)
        while month <= end:
            yield month
            if month.month == 12:
                month = month.replace(year=month.year + 1, month=1)
            else:
                month = month.replace(month=month.month + 1)

    def get_calendar_url_template(self, list_view_soup):
        """
        Build a month-calendar URL template from a real meeting link.

        The month view is only reachable in DNN's path form, which needs
        the site's tabid as well as the module id. Rather than guessing
        either, lift both from a meeting link the list view has already
        given us, and swap in the calendar's ctl name. Returns None if no
        usable link is present, which leaves us on the list view.
        """
        for link in list_view_soup.find_all("a", href=True):
            match = self.CALENDAR_LINK_PATTERN.search(link["href"])
            if not match:
                continue
            return (
                f"{match.group('prefix')}/ctl/MeetingCalendarPublicNoJava"
                f"/{match.group('mid')}/Date/{{date}}/Default.aspx"
            )
        return None

    def parse_calendar_page(self, text, month):
        """
        Parse one month of the no-JS calendar into meeting rows.

        The calendar is a table of day headings ("Monday, 8 June"), each
        followed by a cell holding one link per meeting that day. The day
        cell carries no year, so it's taken from the month requested.
        """
        soup = BeautifulSoup(text, "lxml")
        rows = []

        for cell in soup.find_all("td"):
            day_match = self.CALENDAR_DAY_PATTERN.match(cell.get_text(strip=True))
            if not day_match:
                continue

            entries = cell.find_next_sibling("td")
            if entries is None:
                continue

            try:
                date = datetime.datetime.strptime(
                    "{} {} {}".format(
                        day_match.group("day"),
                        day_match.group("month"),
                        day_match.group("year") or month.year,
                    ),
                    "%d %B %Y",
                ).date()
            except ValueError:
                continue

            for link in entries.find_all("a", href=True):
                url_match = self.ROW_URL_PATTERN.search(link["href"])
                if not url_match:
                    continue
                entry_match = self.CALENDAR_ENTRY_PATTERN.match(
                    link.get_text(strip=True)
                )
                if not entry_match:
                    continue

                rows.append(
                    {
                        "url": link["href"],
                        "meeting_id": url_match.group("meeting_id"),
                        "committee_id": url_match.group("committee_id"),
                        "committee": entry_match.group("committee").strip(),
                        "date": date,
                        "venue": None,
                    }
                )

        return rows

    def parse_list_view(self, soup):
        """Parse the current-month list view. The fallback source."""
        rows = []
        for row in soup.select("tr.rgRow, tr.rgAltRow"):
            parsed = self.parse_row(row)
            if parsed is not None:
                rows.append(parsed)
        return rows

    def get_meetings(self):
        module_id = self.get_module_id()
        list_url = self.format_list_view_url(module_id)
        list_text = self.get_text(list_url, extra_headers=self.extra_headers)
        list_soup = BeautifulSoup(list_text, "lxml")
        start, end = self.date_range

        template = self.get_calendar_url_template(list_soup)
        if template is None:
            self.console.log(
                "[yellow]No CMIS month-calendar link found; falling back to "
                "the current-month list view[/yellow]"
            )
            rows = self.parse_list_view(list_soup)
        else:
            rows = []
            for month in self.months_in_range(start, end):
                url = template.format(date=month.isoformat())
                rows.extend(
                    self.parse_calendar_page(
                        self.get_text(url, extra_headers=self.extra_headers), month
                    )
                )

        # A meeting can appear in more than one source or month page; the
        # meeting id is the stable identifier.
        seen = set()
        in_range = []
        for row in rows:
            if not (start <= row["date"] <= end):
                continue
            if row["meeting_id"] in seen:
                continue
            seen.add(row["meeting_id"])
            in_range.append(row)
        return in_range

    def parse_meeting_summary(self, detail_soup):
        """
        Read the "Meeting Summary" block on a meeting page into a dict of
        label -> value, e.g. {"Status": "Scheduled", "Venue": "Council House"}.

        The block is a run of label/value pairs. Their CSS classes are not
        trustworthy - the same `Value Venue` class is used for the status
        and the venue - so the label text is what identifies each field.
        """
        summary = {}
        for holder in detail_soup.select("div.holder"):
            label_el = holder.select_one("div.DNNLabel")
            value_el = holder.select_one("div.Value")
            if label_el is None or value_el is None:
                continue
            label = label_el.get_text(strip=True).rstrip(":").strip()
            value = value_el.get_text(" ", strip=True)
            if label and value and label not in summary:
                summary[label] = value
        return summary

    def get_documents(self, detail_soup, meeting_date=None):
        documents = []
        seen = set()
        for item in detail_soup.select("li.DocumentListItem"):
            link = item.select_one("a.TitleLink")
            if not link or not link.get("href"):
                continue
            url = urljoin(self.base_url, link["href"])
            if url in seen:
                continue
            seen.add(url)
            title = link.get_text(strip=True)
            documents.append(
                {
                    "title": title,
                    "url": url,
                    "attachment_id": None,
                    "category": self.categorise_document(title, meeting_date),
                }
            )
        return documents

    @staticmethod
    def grid_rows(table):
        """A Telerik RadGrid's data rows as lists of cells, skipping 'no records'."""
        return [
            tr.select("td")
            for tr in table.select("tr.rgRow, tr.rgAltRow")
            if tr.select("td")
        ]

    @staticmethod
    def cmis_member_id(cell):
        """The id in a ViewCMIS_Person/.../id/<id>/ link, as the councillors scraper uses."""
        link = cell.find("a", href=True)
        match = re.search(r"/id/(\d+)/", link["href"]) if link else None
        return match.group(1) if match else None

    def get_attendance(self, detail_soup):
        """
        The Attendance tab: who attended (committee members and others),
        apologies and absences with their reasons, and any substitutes, in
        the same shape as ModernGov's attendance. ``attendance`` is CMIS's
        own heading, e.g. "Attended - Committee Members" or "Apologies".
        """
        tab = detail_soup.select_one(".AttendanceTab")
        if tab is None:
            return []
        attendance = []
        for fieldset in tab.find_all("fieldset"):
            legend = fieldset.find("legend")
            table = fieldset.select_one("table.rgMasterTable")
            if legend is None or table is None:
                continue
            heading = legend.get_text(" ", strip=True)
            columns = [
                th.get_text(" ", strip=True).lower() for th in table.select("th")
            ]
            for cells in self.grid_rows(table):
                values = {
                    column: cell.get_text(" ", strip=True) or None
                    for column, cell in zip(columns, cells)
                }
                name_cell = cells[0]
                link = name_cell.find("a", href=True)
                attendance.append(
                    {
                        "member_id": self.cmis_member_id(name_cell),
                        "member_type": None,
                        "name": name_cell.get_text(" ", strip=True) or None,
                        "role": None,
                        "attendance": heading,
                        "party": None,
                        "ward": None,
                        "key_posts": None,
                        "url": link["href"] if link else None,
                        "reason": next(
                            (v for c, v in values.items() if c.startswith("reason")),
                            None,
                        ),
                        "substituted_by": values.get("substituted by"),
                    }
                )
        return attendance

    def get_declarations(self, detail_soup):
        """The Declarations of Interests table, one entry per declaration."""
        for table in detail_soup.select("table.rgMasterTable"):
            columns = [th.get_text(" ", strip=True) for th in table.select("th")]
            if "Nature of Declaration" not in columns:
                continue
            keys = {
                "Member Name": "member_name",
                "Item Ref.": "item_ref",
                "Details": "details",
                "Nature of Declaration": "nature",
                "Action": "action",
            }
            declarations = []
            for cells in self.grid_rows(table):
                entry = {
                    keys.get(column, column): cell.get_text(" ", strip=True) or None
                    for column, cell in zip(columns, cells)
                }
                entry["member_id"] = self.cmis_member_id(cells[0])
                declarations.append(entry)
            return declarations
        return []

    def get_single_meeting(self, row):
        etag, last_modified = self.validators_for(row["url"])
        response = self.get_conditional(
            row["url"],
            etag=etag,
            last_modified=last_modified,
            extra_headers=self.extra_headers,
        )
        if self.response_status(response) == 304:
            raise MeetingNotModifiedException(row["url"])

        source = self.record_source(row["url"], response)

        detail_text = response.text
        if callable(detail_text):
            detail_text = detail_text()
        detail_soup = BeautifulSoup(detail_text, "lxml")

        meeting = self.add_meeting(
            row["url"],
            identifier=row["meeting_id"],
            title=f"{row['committee']} - {row['date'].isoformat()}",
            committee=row["committee"],
            date=row["date"].isoformat(),
            committee_id=row["committee_id"],
        )
        summary = self.parse_meeting_summary(detail_soup)
        meeting.status = summary.get("Status")
        # The month calendar has no venue column, so prefer the detail
        # page and fall back to whatever the list view gave us.
        meeting.location = summary.get("Venue") or row["venue"]
        meeting.source = source
        meeting.documents = self.get_documents(
            detail_soup, meeting_date=row["date"].isoformat()
        )
        meeting.install_url = self.base_url
        meeting.attendance = self.get_attendance(detail_soup)
        meeting.declarations = self.get_declarations(detail_soup)
        meeting.minutes_published = any(
            document["category"] == "minutes" for document in meeting.documents
        )

        if self.exclude_meeting_hook(meeting):
            raise SkipMeetingException

        content = detail_soup.select_one("#dnn_ContentPane") or detail_soup
        return meeting, content

    def exclude_meeting_hook(self, meeting: MeetingBase):
        return False
