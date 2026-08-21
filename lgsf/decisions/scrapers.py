import abc
import datetime
import json
import re
from functools import cached_property
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup, Tag
from dateutil.relativedelta import relativedelta

from lgsf.aws_lambda.run_log import RunLog
from lgsf.decisions import DecisionBase
from lgsf.decisions.exceptions import (
    DecisionNotModifiedException,
    SkipDecisionException,
)
from lgsf.scrapers import ScraperBase
from lgsf.storage.backends.base import StorageMode


class BaseDecisionsScraper(ScraperBase):
    tags = []
    class_tags = []
    ext = "html"
    scraper_object_type = "Decisions"
    service_name = "decisions"

    # Decision list pages are generated per request over a date range, and a
    # wide range makes some councils time out. See years_back.
    timeout = 60

    # Decisions are an append-only public record: one made last year is still
    # a fact once it drops out of the scrape window, so runs must add to
    # what's stored rather than replacing it.
    storage_mode = StorageMode.ACCUMULATE

    # Decisions are scraped over a window ending today. Unlike meetings there
    # are no forthcoming records to collect here, because only published
    # decisions are scraped.
    years_back = 1

    # A decision is amended, if at all, in the months just after it is
    # published: a status moves on, or a report is attached late. Past that
    # it is settled, so one we already hold is not fetched again. Set to
    # None to re-fetch every decision in the window on every run.
    settled_after_months = 3

    # When True, every document linked from a decision is downloaded to the
    # document store. The decision text itself is not a document: it goes to
    # the metadata store as part of the record.
    save_documents = True

    #: Where the run index lives inside the metadata store.
    INDEX_FILE_NAME = "_index.json"
    INDEX_VERSION = 1

    def __init__(self, options, console):
        super().__init__(options, console)
        self.decisions = set()
        self.new_data = True
        self.unchanged_decisions = 0
        self.settled_decisions = 0
        self.failed_decisions = 0
        self.documents_downloaded = 0
        self.documents_skipped = 0
        self.documents_linked = 0

    @property
    def skip_documents(self):
        """
        True when this run should find documents but not download them.

        Set by --skip-documents, so a sweep can check that decisions are
        found and parsed without transferring every attachment.
        """
        return bool(self.options.get("skip_documents"))

    @cached_property
    def index(self):
        """
        What previous runs already fetched, keyed by URL.

        The storage session API can read and write files but not list them,
        so rather than enumerating stored decision JSON we keep one index
        alongside them. It records the HTTP validators for each decision
        page and, for each document, where it was stored.

        This is a cache, not a source of truth: every entry is verified
        against the document store before it's used to skip work.
        """
        empty = {"version": self.INDEX_VERSION, "decisions": {}, "documents": {}}
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
                "decisions": stored.get("decisions") or {},
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
        entry = self.index["decisions"].get(url) or {}
        return entry.get("etag"), entry.get("last_modified")

    def record_source(self, url, response):
        """
        Note the validators a response came back with, and return the
        ``source`` block stored on the decision.
        """
        source = {
            "url": url,
            "etag": self.response_header(response, "etag"),
            "last_modified": self.response_header(response, "last-modified"),
        }
        # Updated rather than replaced: the entry also carries where the
        # record was stored, which a re-fetch must not discard.
        entry = self.index["decisions"].setdefault(url, {})
        entry["etag"] = source["etag"]
        entry["last_modified"] = source["last_modified"]
        return source

    @property
    def date_range(self):
        today = datetime.date.today()
        start = today - datetime.timedelta(days=365 * self.years_back)
        return start, today

    @property
    def settled_before(self):
        """
        Decisions published before this date are treated as final.

        None when ``settled_after_months`` is unset, meaning nothing is
        treated as settled and every decision in the window is fetched.
        """
        if self.settled_after_months is None:
            return None
        return datetime.date.today() - relativedelta(months=self.settled_after_months)

    def stored_decision(self, url):
        """
        The stored record's filename for ``url``, if we really hold it.

        The index is a cache, not a source of truth, so the file it names is
        opened before its word is taken: a record deleted from storage costs
        a re-fetch rather than being lost.
        """
        file_name = (self.index["decisions"].get(url) or {}).get("file_name")
        if not file_name:
            return None
        try:
            self.storage_session.open(Path("json") / file_name)
        except (FileNotFoundError, ValueError):
            return None
        return file_name

    def is_settled(self, row):
        """
        True when a decision is old enough not to change again, and we
        already hold it.

        Publication date is what matters here, not the date the decision was
        taken: a decision published last week may have been made years ago,
        and it is the publishing that starts the clock on amendments. The
        list page carries it, so this is decided without fetching anything.
        """
        cutoff = self.settled_before
        if cutoff is None:
            return False

        published = row.get("published_date")
        if not published:
            # Without a date there is no way to know it has settled.
            return False

        if published >= cutoff.isoformat():
            return False

        return bool(self.stored_decision(row["url"]))

    @abc.abstractmethod
    def get_decisions(self):
        """Yield one raw item per decision, for get_single_decision()."""

    @abc.abstractmethod
    def get_single_decision(self, decision_data):
        """Turn one raw item into a decision via self.add_decision()."""

    def add_decision(
        self,
        url,
        identifier: str,
        title: str,
        date: str,
        decision_maker: str = None,
    ):
        assert title, f"No Title for {url}"
        assert date, f"No Date for {url}"
        assert identifier, f"No Identifier for {url}"
        decision = DecisionBase(
            url,
            identifier=identifier,
            title=title,
            date=date,
            decision_maker=decision_maker,
        )
        self.decisions.add(decision)
        return decision

    @property
    def get_tags(self):
        return self.tags + self.class_tags

    @property
    def report_items(self):
        return list(self.decisions)

    def run(self, run_log: RunLog):
        for decision_data in self.get_decisions():
            if self.is_settled(decision_data):
                self.settled_decisions += 1
                continue
            try:
                decision = self.get_single_decision(decision_data)
                self.process_decision(decision, decision_data)
            except DecisionNotModifiedException:
                self.unchanged_decisions += 1
                continue
            except SkipDecisionException:
                continue
            except Exception as e:
                # Each decision is a separate page fetch and a council can
                # have hundreds in the window, so one failing page is
                # expected rather than exceptional. Letting it propagate
                # would abandon the run before finalize_storage, losing
                # every decision scraped so far.
                self.failed_decisions += 1
                self.console.log(
                    f"[yellow]Failed to scrape {decision_data.get('url')}: {e}[/yellow]"
                )
                continue

        self.save_index()

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

    def prettify_decision_str(self, decision_raw_str):
        if isinstance(decision_raw_str, dict):
            return json.dumps(decision_raw_str, indent=4)
        if isinstance(decision_raw_str, Tag):
            return decision_raw_str.prettify()
        if isinstance(decision_raw_str, str):
            return decision_raw_str
        return None

    def process_decision(self, decision, decision_raw_str):
        formatted = self.prettify_decision_str(decision_raw_str)

        self.documents_linked += len(getattr(decision, "documents", []) or [])

        # Documents first: storing them adds the hash and storage key to each
        # document dict, and the decision JSON is serialised from those, so
        # saving the decision first would record documents with no location.
        if self.save_documents and not self.skip_documents:
            self.save_decision_documents(decision)

        self.save_decision(formatted, decision)

    def save_decision(self, raw_content, decision_obj):
        assert type(decision_obj) is DecisionBase, (
            "Scrapers must return a decision object"
        )
        file_name = "{}.{}".format(decision_obj.as_file_name(), self.ext)
        self.save_raw(file_name, raw_content)
        self.save_json(decision_obj)

        # Note where the record went, so a later run can tell that it really
        # holds this decision before deciding not to fetch it again.
        entry = self.index["decisions"].setdefault(decision_obj.url, {})
        entry["file_name"] = "{}.json".format(decision_obj.as_file_name())

    def document_key(self, decision_obj, document, index):
        """
        Build the document store key for one of a decision's documents.

        ModernGov document URLs carry an ``sNNNN`` id that is stable, but not
        every source has one, so fall back to a per-decision index to keep
        keys distinct.
        """
        suffix = document.get("attachment_id") or index
        extension = self.document_extension(document)
        return "{}-{}{}".format(decision_obj.as_file_name(), suffix, extension)

    def document_extension(self, document):
        """
        Guess a file extension from the document URL.

        ModernGov serves some documents through handler URLs with no
        extension at all, and those are overwhelmingly PDFs, so that's the
        fallback.
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

    def save_decision_documents(self, decision_obj):
        """
        Store every document linked from this decision, skipping any we
        already hold, and record where each one went on the decision.

        A failed download shouldn't fail the whole scrape.
        """
        for index, document in enumerate(
            getattr(decision_obj, "documents", []), start=1
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

            stale = self.index["documents"].get(url) or {}
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

            key = self.document_key(decision_obj, document, index)
            stored = self.document_storage.write(key, content)

            entry = stored.as_dict()
            entry["etag"] = self.response_header(response, "etag")
            entry["last_modified"] = self.response_header(response, "last-modified")
            entry["content_type"] = self.response_header(response, "content-type")

            document.update(entry)
            self.index["documents"][url] = entry
            self.documents_downloaded += 1

    def report(self):
        # A council can legitimately publish no decisions in the window, so
        # don't treat a low count as an error.
        self.console.log(
            f"Found {len(self.decisions)} decisions "
            f"({self.unchanged_decisions} unchanged since last run, "
            f"{self.settled_decisions} already stored and settled)"
        )
        if self.failed_decisions:
            self.console.log(
                f"[yellow]{self.failed_decisions} decision pages could not be "
                f"read and were skipped[/yellow]"
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


class ModGovDecisionsScraper(BaseDecisionsScraper):
    """
    Decisions from a ModernGov install.

    There is no decisions method on ModernGov's web service — it covers
    meetings, committees and councillors only — so unlike the minutes
    scraper this reads the rendered HTML.

    Two list pages carry decisions and councils differ in which they
    populate, so both are read and the results deduplicated by URL:

    - ``mgDelegatedDecisions.aspx`` with ``DS=2``, published decisions
    - ``mgListOfficerDecisions.aspx``, officer decisions
    """

    class_tags = ["modgov"]

    DELEGATED_PATH = "mgDelegatedDecisions.aspx"
    OFFICER_PATH = "mgListOfficerDecisions.aspx"

    #: Decision status to request from the delegated decisions page.
    #: 2 is "published", meaning decisions actually made. Forthcoming
    #: decisions (1) are proposals and are deliberately not scraped.
    DECISION_STATUS = 2

    def format_date(self, date):
        """ModernGov wants dd/mm/yyyy, slash-encoded, in its DR parameter."""
        return date.strftime("%d%%2f%m%%2f%Y")

    def list_urls(self):
        """
        The list pages to read, each paired with whether it is the officer
        list.

        The officer page returns a subset of the delegated one — an officer
        decision is a delegated decision whose delegate is an officer — so
        it adds no coverage. It is still read, because appearing on it is
        the only signal the source gives for which decisions those are.
        """
        start, end = self.date_range
        date_range = f"{self.format_date(start)}-{self.format_date(end)}"

        delegated = (
            f"{self.DELEGATED_PATH}?XXR=0&DR={date_range}&ACT=Find&RP=0&K=0"
            f"&V=0&DM=0&HD=0&DS={self.DECISION_STATUS}&Next=true"
            f"&META=mgdelegateddecisions"
        )
        officer = (
            f"{self.OFFICER_PATH}?XXR=0&FULL=1&BAM=1&DR={date_range}"
            f"&ACT=Find&K=0&DM=0&DEP=0&PH=0&LO=0&C3=0&Next=true"
        )
        return [
            (urljoin(self.base_url, delegated), False),
            (urljoin(self.base_url, officer), True),
        ]

    def get_page(self, url):
        return BeautifulSoup(
            self.get_text(url, extra_headers=self.extra_headers), "lxml"
        )

    def decision_rows(self, soup, list_url):
        """
        Yield one dict per decision row on a list page.

        The link carries the decision id; the row's second cell carries the
        published date. A row's link text ends with a hidden ``ref: NNNN``
        span which is chrome, not part of the title.
        """
        for link in soup.select('a[href*="ieDecisionDetails.aspx"]'):
            href = link.get("href")
            if not href:
                continue

            url = urljoin(list_url, href)
            identifier = parse_qs(urlparse(url).query).get("ID", [None])[0]
            if not identifier:
                continue

            for hidden in link.select(".mgHide"):
                hidden.decompose()

            row = link.find_parent("tr")
            cells = row.find_all("td") if row else []
            date = cells[1].get_text(strip=True) if len(cells) > 1 else None

            yield {
                "url": url,
                "identifier": identifier,
                "title": link.get_text(strip=True),
                # The list's date column is when the decision was published,
                # which is not when it was taken. The detail page carries
                # both; this is the fallback for the second.
                "published_date": self.iso_date(date),
            }

    def iso_date(self, date_str):
        """dd/mm/yyyy as ModernGov writes it, to an ISO date string."""
        if not date_str:
            return None
        try:
            return (
                datetime.datetime.strptime(date_str.strip(), "%d/%m/%Y")
                .date()
                .isoformat()
            )
        except ValueError:
            return None

    def get_decisions(self):
        """
        Every decision in the window, from both list pages.

        Both pages are read before any is yielded, so that a decision
        appearing on the officer list can be marked as one however it was
        found first.

        A council can legitimately not have one of the two pages, so a
        failure to read one is logged and the other still runs.
        """
        rows = {}
        officer_urls = set()

        for list_url, is_officer_list in self.list_urls():
            try:
                soup = self.get_page(list_url)
            except Exception as e:
                self.console.log(f"[yellow]Could not read {list_url}: {e}[/yellow]")
                continue

            for row in self.decision_rows(soup, list_url):
                if is_officer_list:
                    officer_urls.add(row["url"])
                rows.setdefault(row["url"], row)

        for url, row in rows.items():
            row["is_officer_decision"] = url in officer_urls
            yield row

    def label_value(self, soup, label):
        """
        The text following a ``mgLabel`` span, by label text.

        Labels vary between installs — a decision may carry "Decision
        Maker:" or "Decided at meeting:" — so callers ask for what they
        want and get None when it isn't there.
        """
        wanted = label.lower().rstrip(": ")
        for span in soup.select("span.mgLabel"):
            if span.get_text(strip=True).lower().rstrip(": ") != wanted:
                continue
            parent = span.find_parent("p")
            if not parent:
                continue
            value = parent.get_text(" ", strip=True)
            prefix = span.get_text(" ", strip=True)
            return value[len(prefix) :].strip(" :") or None
        return None

    def section_text(self, soup, heading):
        """
        The body of a ``mgSubSubTitleTxt`` section, by its heading.

        A decision page carries its text in one of these, and often a
        Purpose in another, both wrapped in a WordSection1 block.
        """
        wanted = heading.lower().rstrip(": ")
        for title in soup.select("h3.mgSubSubTitleTxt"):
            if title.get_text(strip=True).lower().rstrip(": ") != wanted:
                continue
            block = title.find_next_sibling()
            if block is None:
                return None
            return self.block_text(block)
        return None

    def block_text(self, block):
        """
        A block of Word-exported HTML as plain text.

        Decision text is pasted from Word and hard-wrapped in the source, so
        reading it straight puts newlines mid-sentence. Paragraphs are read
        one at a time with their internal whitespace collapsed, keeping the
        breaks between paragraphs and dropping the blank ones that are only
        there for spacing.
        """
        paragraphs = block.find_all("p")
        if not paragraphs:
            return " ".join(block.get_text(" ", strip=True).split()) or None

        out = []
        for paragraph in paragraphs:
            text = " ".join(paragraph.get_text(" ", strip=True).split())
            if text:
                out.append(text)
        return "\n\n".join(out) or None

    def get_documents(self, soup, page_url):
        """
        Documents linked from a decision page.

        Scoped to the bullet list ModernGov puts under "Accompanying
        Documents", so the site's own navigation links aren't collected.
        The ``sNNNN`` segment of the path is a stable attachment id.
        """
        documents = []
        for bullet in soup.select("ul.mgBulletList a[href]"):
            href = bullet.get("href")
            if not href:
                continue

            for size in bullet.select(".mgFileSize"):
                size.decompose()

            url = urljoin(page_url, href)
            attachment_id = None
            match = re.search(r"/(s\d+)/", urlparse(url).path)
            if match:
                attachment_id = match.group(1)

            documents.append(
                {
                    "title": bullet.get_text(" ", strip=True),
                    "url": url,
                    "attachment_id": attachment_id,
                }
            )
        return documents

    def get_single_decision(self, decision_data):
        """Fetch a decision's own page and read the record off it."""
        url = decision_data["url"]
        etag, last_modified = self.validators_for(url)
        response = self.get_conditional(
            url,
            etag=etag,
            last_modified=last_modified,
            extra_headers=self.extra_headers,
        )
        if self.response_status(response) == 304:
            raise DecisionNotModifiedException(url)

        text = response.text
        if callable(text):
            text = text()
        soup = BeautifulSoup(text, "lxml")

        title = decision_data.get("title")
        heading = soup.select_one("h2.mgSubTitleTxt")
        if heading:
            title = heading.get_text(" ", strip=True) or title

        date = self.iso_date(self.label_value(soup, "Date of decision")) or (
            decision_data.get("published_date")
        )
        if not date:
            raise SkipDecisionException(f"No date for {url}")

        decision = self.add_decision(
            url,
            identifier=decision_data["identifier"],
            title=title,
            date=date,
            decision_maker=self.label_value(soup, "Decision Maker")
            or self.label_value(soup, "Decided at meeting"),
        )
        decision.status = self.label_value(soup, "Decision status")
        decision.is_key_decision = self.yes_no(
            self.label_value(soup, "Is Key decision?")
        )
        decision.is_subject_to_call_in = self.yes_no(
            self.label_value(soup, "Is subject to call in?")
        )
        decision.is_officer_decision = decision_data.get("is_officer_decision")
        decision.publication_date = self.iso_date(
            self.label_value(soup, "Publication date")
        )
        decision.purpose = self.section_text(soup, "Purpose")
        decision.text = self.section_text(soup, "Decision")
        decision.documents = self.get_documents(soup, url)
        decision.source = self.record_source(url, response)
        return decision

    def yes_no(self, value):
        """ModernGov writes these as Yes/No text, not a flag."""
        if value is None:
            return None
        return value.strip().lower() == "yes"


class CustomHTMLDecisionsScraper(BaseDecisionsScraper):
    """
    Base for councils whose decisions are on neither ModernGov nor CMIS.

    Supplies page fetching and parsing. `get_decisions` and
    `get_single_decision` are still abstract, and are what the council's
    scraper implements.
    """

    class_tags = ["html"]

    def get_page(self, url):
        """The URL fetched and parsed, ready to select against."""
        return BeautifulSoup(
            self.get_text(url, extra_headers=self.extra_headers), "lxml"
        )
