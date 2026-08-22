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

    def __init__(self, options, console):
        super().__init__(options, console)
        self.interests_registers = set()
        self.new_data = True

    @property
    def get_tags(self):
        return self.tags + self.class_tags

    @property
    def report_items(self):
        return list(self.interests_registers)

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

    def process_interests_register(self, record: RegisterOfInterestsBase, raw: str):
        self.save_raw(f"{record.as_file_name()}.{self.ext}", raw)
        self.save_json(record)

    def report(self):
        self.console.log(
            f"Found {len(self.interests_registers)} registers of interests"
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

    def get_single_interests_register(self, ward, councillor_xml):
        councillor_id = councillor_xml.find("councillorid").text
        councillor_name = councillor_xml.find("fullusername").text
        division = getattr(ward.find("wardtitle"), "text", None)
        councillor_url = f"{self.base_url}/mgUserInfo.aspx?UID={councillor_id}"
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
                if cells:
                    rows.append(cells)

            categories.append(
                {
                    "category": caption,
                    "headers": headers,
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
            href = a["href"]
            if re.search(r"\.(pdf|docx?)$", href, re.IGNORECASE):
                doc_url = urljoin(self.base_url, href)
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
