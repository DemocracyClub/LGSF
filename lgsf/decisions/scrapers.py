import abc
import datetime
import json
from functools import cached_property

from bs4 import BeautifulSoup, Tag

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
        self.index["decisions"][url] = {
            "etag": source["etag"],
            "last_modified": source["last_modified"],
        }
        return source

    @property
    def date_range(self):
        today = datetime.date.today()
        start = today - datetime.timedelta(days=365 * self.years_back)
        return start, today

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
            try:
                decision = self.get_single_decision(decision_data)
                self.process_decision(decision, decision_data)
            except DecisionNotModifiedException:
                self.unchanged_decisions += 1
                continue
            except SkipDecisionException:
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
            f"({self.unchanged_decisions} unchanged since last run)"
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
