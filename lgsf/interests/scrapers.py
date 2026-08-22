import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from lgsf.aws_lambda.run_log import RunLog
from lgsf.interests.exceptions import SkipInterestsException
from lgsf.interests.models import RegisterOfInterestsBase
from lgsf.scrapers import ScraperBase
from lgsf.storage.backends.base import StorageMode


class BaseInterestsScraper(ScraperBase):
    tags = []
    class_tags = []
    ext = "html"
    scraper_object_type = "Interests"
    service_name = "interests"
    storage_mode = StorageMode.REPLACE
    save_documents = True

    def __init__(self, options, console):
        super().__init__(options, console)
        self.interests_registers = set()
        self.new_data = True
        self.documents_downloaded = 0
        self.documents_skipped = 0
        self.documents_linked = 0

    @property
    def skip_documents(self):
        """
        True when this run should find documents but not download them.
        """
        return bool(self.options.get("skip_documents"))

    @property
    def get_tags(self):
        return self.tags + self.class_tags

    @property
    def report_items(self):
        return list(self.interests_registers)

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

    def add_interests_register(
        self,
        url: str,
        identifier: str,
        councillor_name: str,
        councillor_id: str,
        councillor_url: str = None,
        division: str = None,
    ) -> RegisterOfInterestsBase:
        assert councillor_name, f"No councillor name for {url}"
        assert identifier, f"No identifier for {url}"
        record = RegisterOfInterestsBase(
            url=url,
            identifier=identifier,
            councillor_name=councillor_name,
            councillor_id=councillor_id,
            councillor_url=councillor_url,
            division=division,
        )
        self.interests_registers.add(record)
        return record

    def document_extension(self, document):
        """
        Guess a file extension from the document URL.

        ModernGov serves interests docs through handler URLs with no extension
        (mgConvert2PDF.aspx), which are overwhelmingly PDFs, so that's the
        fallback — matching the minutes scraper convention.
        """
        path = document.get("url", "").split("?")[0].lower()
        for extension in (".pdf", ".docx", ".doc", ".xlsx", ".xls", ".rtf", ".txt"):
            if path.endswith(extension):
                return extension
        return ".pdf"

    def save_interest_documents(self, record: RegisterOfInterestsBase):
        """
        Store documents linked to this register of interests in document_storage.

        Skips documents that are already in the store (keyed by councillor +
        index). A failed download logs a warning but does not fail the whole
        record — matching the minutes scraper behaviour.

        When a document is skipped because it's already stored, its metadata
        (storage_key, content_hash, etc.) is still recovered via describe() so
        the JSON looks identical whether the file was fetched this run or a
        previous one.
        """
        documents = getattr(record, "documents", []) or []
        self.documents_linked += len(documents)

        if self.skip_documents or not documents:
            return

        for index, document in enumerate(documents, start=1):
            url = document.get("url")
            if not url:
                continue

            key = f"{record.as_file_name()}-{index}{self.document_extension(document)}"

            # Skip what we already have, but still backfill storage metadata
            # so the JSON looks the same as a freshly downloaded document.
            if self.document_storage.exists(key):
                stored = self.document_storage.describe(key)
                if stored:
                    document.update(stored.as_dict())
                self.documents_skipped += 1
                continue

            try:
                content = self.get_bytes(url)
                stored = self.document_storage.write(key, content)
                document.update(stored.as_dict())
                self.documents_downloaded += 1
            except Exception as e:
                self.console.log(
                    f"[yellow]Failed to download document {url}: {e}[/yellow]"
                )

    def process_interests_register(self, record: RegisterOfInterestsBase, raw: str):
        self.save_interest_documents(record)
        self.save_raw(f"{record.as_file_name()}.{self.ext}", raw)
        self.save_json(record)

    def report(self):
        self.console.log(
            f"Found {len(self.interests_registers)} registers of interests"
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


class ModGovInterestsScraper(BaseInterestsScraper):
    class_tags = ["modgov"]

    def format_councillors_api_url(self):
        return f"{self.base_url}/mgWebService.asmx/GetCouncillorsByWard"

    def get_interests_registers(self):
        text = self.get_text(
            self.format_councillors_api_url(), extra_headers=self.extra_headers
        )
        soup = BeautifulSoup(text, features="xml")
        return soup.find_all("ward")

    def discover_rofi_link(self, user_url):
        """
        Inspect councillor's profile page (mgUserInfo.aspx) to discover the
        Register of Interests link (which may be an HTML page or a PDF).
        """
        try:
            user_html = self.get_text(user_url, extra_headers=self.extra_headers)
        except Exception:
            return None

        soup = BeautifulSoup(user_html, "html.parser")
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a["href"].lower()
            # Ignore meeting-specific declarations
            if (
                "declarations at meetings" in text
                or "mglistdeclarationsofinterest" in href
            ):
                continue
            if (
                "register of interests" in text
                or "declarations of interest" in text
                or "register of member interests" in text
                or "mgrofi.aspx" in href
                or "mgconvert2pdf.aspx" in href
                or ("interest" in text and (".pdf" in href or "convert2pdf" in href))
            ):
                return a
        return None

    def get_single_interests_register(self, ward, councillor_xml):
        councillor_id = councillor_xml.find("councillorid").text
        councillor_name = councillor_xml.find("fullusername").text
        division = getattr(ward.find("wardtitle"), "text", None)
        councillor_url = f"{self.base_url}/mgUserInfo.aspx?UID={councillor_id}"

        # 1. Discover link from councillor's profile
        rofi_link = self.discover_rofi_link(councillor_url)

        if rofi_link:
            href = rofi_link["href"]
            link_text = rofi_link.get_text(strip=True)
            rofi_url = urljoin(self.base_url, href)

            # Check if this is a PDF or dynamic PDF conversion (like Aberdeenshire)
            if (
                href.lower().endswith((".pdf", ".docx", ".doc"))
                or "mgconvert2pdf" in href.lower()
            ):
                record = self.add_interests_register(
                    url=rofi_url,
                    identifier=councillor_id,
                    councillor_name=councillor_name,
                    councillor_id=councillor_id,
                    councillor_url=councillor_url,
                    division=division,
                )
                record.interests = []
                record.published_date = None
                record.documents = [
                    {
                        "title": link_text or "Register of interests PDF",
                        "url": rofi_url,
                    }
                ]
                raw_data = (
                    f"<!-- PDF Register of Interests -->\nURL: {rofi_url}\n"
                    f"Title: {link_text}\n"
                )
                return record, raw_data

            # Otherwise, fetch and parse HTML
            try:
                raw_html = self.get_text(rofi_url, extra_headers=self.extra_headers)
            except Exception:
                raise SkipInterestsException()
        else:
            # Fallback: Try direct mgRofI.aspx endpoint
            rofi_url = f"{self.base_url}/mgRofI.aspx?UID={councillor_id}"
            try:
                raw_html = self.get_text(rofi_url, extra_headers=self.extra_headers)
            except Exception:
                raise SkipInterestsException()

        soup = BeautifulSoup(raw_html, "html.parser")
        tables = soup.select("table.mgInterestsTable")

        categories = []
        for table in tables:
            caption_elem = table.find("caption")
            caption = (
                caption_elem.get_text(strip=True)
                if caption_elem
                else table.get("summary", "Interest Category")
            )
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            rows = []
            for tr in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if not cells:
                    continue
                if len(headers) == len(cells):
                    # Map each cell to its column header so the relationship
                    # is explicit: {"You": "Retired", "Spouse/partner": "N/A"}
                    rows.append(dict(zip(headers, cells)))
                else:
                    # Header count doesn't match cells (shouldn't happen in
                    # practice, but degrade gracefully).
                    rows.append(cells)

            categories.append(
                {
                    "category": caption,
                    "rows": rows,
                }
            )

        # Published date parsing
        published_date = None
        text_content = soup.get_text()
        date_match = re.search(
            r"published on\s+([A-Za-z]+,\s+\d+\s+[A-Za-z]+\s+\d{4}[^\n\r<]*?)(?:\.\s*(?:More|Printer|<|$|\n)|\.\s*$|\n)",
            text_content,
            re.IGNORECASE,
        )
        if date_match:
            published_date = date_match.group(1).strip().rstrip(".")

        # Document links
        documents = []
        for a in soup.find_all("a", href=True):
            doc_href = a["href"]
            if re.search(r"\.(pdf|docx?)$", doc_href, re.IGNORECASE):
                doc_url = urljoin(self.base_url, doc_href)
                documents.append(
                    {
                        "title": a.get_text(strip=True) or "Declaration Document",
                        "url": doc_url,
                    }
                )

        record = self.add_interests_register(
            url=rofi_url,
            identifier=councillor_id,
            councillor_name=councillor_name,
            councillor_id=councillor_id,
            councillor_url=councillor_url,
            division=division,
        )
        record.interests = categories
        record.published_date = published_date
        record.documents = documents

        return record, raw_html

    def run(self, run_log: RunLog):
        wards = self.get_interests_registers()
        for ward in wards:
            for councillor_xml in ward.find_all("councillor"):
                try:
                    record, raw_html = self.get_single_interests_register(
                        ward, councillor_xml
                    )
                    self.process_interests_register(record, raw_html)
                except SkipInterestsException:
                    continue

        self.finalize_storage(run_log)
        self.report()
