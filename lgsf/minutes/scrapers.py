import abc
import datetime
import json
import re
from functools import cached_property
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from lgsf.aws_lambda.run_log import RunLog
from lgsf.minutes import MeetingBase
from lgsf.minutes.exceptions import (
    MeetingNotModifiedException,
    SkipMeetingException,
)
from lgsf.scrapers import ScraperBase
from lgsf.storage.backends.base import StorageMode


class BaseMinutesScraper(ScraperBase):
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

    #: Where the run index lives inside the metadata store.
    INDEX_FILE_NAME = "_index.json"
    INDEX_VERSION = 1

    def __init__(self, options, console):
        super().__init__(options, console)
        self.meetings = set()
        self.new_data = True
        self.unchanged_meetings = 0
        self.documents_downloaded = 0
        self.documents_skipped = 0
        self.documents_linked = 0

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
        today = datetime.date.today()
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

    def response_bytes(self, response):
        """
        Return a response body as bytes, normalising across the supported
        HTTP clients (requests/httpx expose .content, wreq a .bytes()).
        """
        for attr in ("content", "bytes"):
            body = getattr(response, attr, None)
            if body is None:
                continue
            if callable(body):
                body = body()
            return body
        raise ValueError(f"Can't read bytes from {type(response)} response")

    def get_bytes(self, url):
        """Wraps self.get and always returns the response body as bytes."""
        return self.response_bytes(self.get(url))

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
        if self.save_documents and not self.skip_documents:
            self.save_meeting_documents(meeting)

        self.save_meeting(formatted_meeting_raw_str, meeting)

    def save_meeting(self, raw_content, meeting_obj):
        assert type(meeting_obj) is MeetingBase, "Scrapers must return a meeting object"
        file_name = "{}.{}".format(meeting_obj.as_file_name(), self.ext)
        self.save_raw(file_name, raw_content)
        self.save_json(meeting_obj)

    def document_key(self, meeting_obj, document, index):
        """
        Build the document store key for one of a meeting's documents.

        CMIS documents have no clean attachment id (their URLs are opaque
        encrypted query strings), so fall back to a per-meeting index to
        keep keys distinct.
        """
        suffix = document.get("attachment_id") or index
        extension = self.document_extension(document)
        return "{}-{}{}".format(meeting_obj.as_file_name(), suffix, extension)

    def document_extension(self, document):
        """
        Guess a file extension from the document URL.

        ModernGov and CMIS both serve documents through handler URLs with no
        extension at all (mgConvert2PDF.aspx, Document.ashx), and those are
        overwhelmingly PDFs, so that's the fallback.
        """
        path = document.get("url", "").split("?")[0].lower()
        for extension in (".pdf", ".docx", ".doc", ".xlsx", ".xls", ".rtf", ".txt"):
            if path.endswith(extension):
                return extension
        return ".pdf"

    def stored_document_metadata(self, url):
        """
        Return the recorded metadata for an already-stored document, or None.

        The index is only trusted as far as the document store agrees with
        it: an entry whose file has gone is treated as never fetched.
        """
        entry = self.index["documents"].get(url)
        if not entry:
            return None

        key = entry.get("storage_key")
        if not key:
            return None

        try:
            if not self.document_storage.exists(key):
                return None
        except ValueError:
            # Key written by a backend with a different key format
            return None

        return entry

    def save_meeting_documents(self, meeting_obj):
        """
        Store every document linked from this meeting, skipping any we
        already hold, and record where each one went on the meeting's
        metadata.

        Documents go to the document store rather than the metadata store:
        the JSON that lands in git carries a hash and a storage key, and the
        file itself lives wherever the document backend puts it.

        A failed download shouldn't fail the whole scrape.
        """
        for index, document in enumerate(
            getattr(meeting_obj, "documents", []), start=1
        ):
            url = document.get("url")
            if not url:
                continue

            existing = self.stored_document_metadata(url)
            if existing:
                # Already downloaded by an earlier run and still in the
                # store: reuse its metadata so a skipped document looks
                # exactly like a freshly fetched one.
                document.update(existing)
                self.documents_skipped += 1
                continue

            etag, last_modified = (None, None)
            stale = self.index["documents"].get(url) or {}
            if stale:
                etag = stale.get("etag")
                last_modified = stale.get("last_modified")

            try:
                response = self.get_conditional(
                    url,
                    etag=etag,
                    last_modified=last_modified,
                    extra_headers=self.extra_headers,
                )
                if self.response_status(response) == 304:
                    # Unchanged, but the file isn't in the store (or we
                    # wouldn't be here), so fetch it unconditionally.
                    response = self.get(url, extra_headers=self.extra_headers)
                content = self.response_bytes(response)
            except Exception as e:
                self.console.log(f"[yellow]Failed to download {url}: {e}[/yellow]")
                continue

            key = self.document_key(meeting_obj, document, index)
            stored = self.document_storage.write(key, content)

            entry = stored.as_dict()
            entry["etag"] = self.response_header(response, "etag")
            entry["last_modified"] = self.response_header(response, "last-modified")
            entry["content_type"] = self.response_header(response, "content-type")

            document.update(entry)
            self.index["documents"][url] = entry
            self.documents_downloaded += 1

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
                f"Documents: {self.documents_downloaded} downloaded, "
                f"{self.documents_skipped} already stored"
            )

    def categorise_document(self, title):
        lowered = title.lower()
        if "minute" in lowered:
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
        for committee in self.get_meetings():
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
        meeting.documents = self.get_documents(detail)

        self.save_minutes_html(meeting, detail)

        if self.exclude_meeting_hook(meeting):
            raise SkipMeetingException

        return meeting

    def parse_date(self, date_str):
        """ModernGov dates are dd/mm/yyyy; store ISO format."""
        return datetime.datetime.strptime(date_str, self.DATE_FORMAT).date().isoformat()

    def get_documents(self, detail):
        documents = []
        seen = set()
        for linked_doc in detail.find_all("linkeddoc"):
            url_tag = linked_doc.find("url")
            title_tag = linked_doc.find("title")
            attachment_id_tag = linked_doc.find("attachmentid")
            if not url_tag or not url_tag.text.strip():
                continue
            url = url_tag.text.strip()
            if url in seen:
                continue
            seen.add(url)
            title = title_tag.text.strip() if title_tag else ""
            documents.append(
                {
                    "title": title,
                    "url": url,
                    "attachment_id": attachment_id_tag.text.strip()
                    if attachment_id_tag
                    else None,
                    "category": self.categorise_document(title),
                }
            )
        return documents

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

    def get_documents(self, detail_soup):
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
                    "category": self.categorise_document(title),
                }
            )
        return documents

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
        meeting.documents = self.get_documents(detail_soup)
        meeting.minutes_published = any(
            document["category"] == "minutes" for document in meeting.documents
        )

        if self.exclude_meeting_hook(meeting):
            raise SkipMeetingException

        content = detail_soup.select_one("#dnn_ContentPane") or detail_soup
        return meeting, content

    def exclude_meeting_hook(self, meeting: MeetingBase):
        return False
