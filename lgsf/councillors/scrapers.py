import abc
import contextlib
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from dateutil.parser import parse

from lgsf.aws_lambda.run_log import RunLog
from lgsf.councillors import CouncillorBase
from lgsf.councillors.exceptions import SkipCouncillorException
from lgsf.scrapers import CodeCommitMixin, ScraperBase


class BaseCouncillorScraper(CodeCommitMixin, ScraperBase):
    tags = []
    class_tags = []
    ext = "html"
    scraper_object_type = "Councillors"

    def __init__(self, options, console):
        super().__init__(options, console)
        self.councillors = set()
        self.new_data = True

    @abc.abstractmethod
    def get_councillors(self):
        pass

    @abc.abstractmethod
    def get_single_councillor(self, councillor_html):
        pass

    def add_councillor(
        self, url, identifier: str, name: str, division: str, party: str
    ):
        assert division, f"No Division for {url}"
        assert name, f"No Name for {url}"
        assert party, f"No Party for {url}"
        assert identifier, f"No Identifier for {url}"
        councillor = CouncillorBase(
            url,
            identifier=identifier,
            name=name,
            division=division,
            party=party,
        )
        self.councillors.add(councillor)
        return councillor

    @property
    def get_tags(self):
        return self.tags + self.class_tags

    def run(self, run_log: RunLog):
        if self.options.get("aws_lambda"):
            self.delete_data_if_exists()
        else:
            self.clean_data_dir()

        for councillor_html in self.get_councillors():
            try:
                councillor = self.get_single_councillor(councillor_html)
                self.process_councillor(councillor, councillor_html)
            except SkipCouncillorException:
                continue

        self.aws_tidy_up(run_log)

        self.report()

    def prettify_councillor_str(self, councillor_raw_str):
        if isinstance(councillor_raw_str, dict):
            return json.dumps(councillor_raw_str, indent=4)
        if isinstance(councillor_raw_str, Tag):
            return councillor_raw_str.prettify()
        return None

    def process_councillor(self, councillor, councillor_raw_str):
        formatted_councillor_raw_str = self.prettify_councillor_str(councillor_raw_str)

        if self.options.get("aws_lambda"):
            # stage...
            self.stage_councillor(formatted_councillor_raw_str, councillor)

            # Do a batch commit if needed...
            if len(self.put_files) > 90:
                self.process_batch()
        else:
            self.save_councillor(formatted_councillor_raw_str, councillor)

    def stage_councillor(self, councillor_data_string, councillor):
        self.options["council"]
        json_file_path = (
            f"{self.scraper_object_type}/json/{councillor.as_file_name()}.json"
        )
        raw_file_path = (
            f"{self.scraper_object_type}/raw/{councillor.as_file_name()}.html"
        )
        self.put_files.extend(
            [
                {
                    "filePath": json_file_path,
                    "fileContent": bytes(
                        json.dumps(councillor.as_dict(), indent=4), "utf-8"
                    ),
                },
                {
                    "filePath": raw_file_path,
                    "fileContent": bytes(councillor_data_string, "utf-8"),
                },
            ]
        )

    def save_councillor(self, raw_content, councillor_obj):
        assert type(councillor_obj) is CouncillorBase, (
            "Scrapers must return a councillor object"
        )
        file_name = "{}.{}".format(councillor_obj.as_file_name(), self.ext)
        self.save_raw(file_name, raw_content)
        self.save_json(councillor_obj)

    def report(self):
        if self.options.get("verbose"):
            if len(self.councillors) < 10:
                raise ValueError(
                    "Not many councillors found ({})".format(len(self.councillors))
                )
            if self.new_data:
                self.console.log(
                    f"Found {len(self.councillors)} councillors with some new data"
                )
            else:
                self.console.log(
                    f"Found {len(self.councillors)} councillors but no new data"
                )


class HTMLCouncillorScraper(BaseCouncillorScraper):
    class_tags = ["html"]

    def get_page(self, url):
        page = self.get(url, extra_headers=self.extra_headers).text
        return BeautifulSoup(page, "html5lib")

    def get_list_container(self):
        """
        Uses :func:`ScraperBase.get <ScraperBase.get>` to request the
        `base_url` and selects the HTML using the selector defined in
        `list_page['container_css_selector']`

        :return: A :class:`BeautifulSoup` object
        """
        self.base_url_soup = self.get_page(self.base_url)
        selected = self.base_url_soup.select(self.list_page["container_css_selector"])
        if len(selected) > 1:
            raise ValueError("More than one element selected")
        return selected[0]

    def get_councillors(self):
        container = self.get_list_container()
        return container.select(self.list_page["councillor_css_selector"])


class PagedHTMLCouncillorScraper(HTMLCouncillorScraper):
    def get_next_link(self, soup):
        try:
            return urljoin(
                self.base_url,
                soup.select_one(self.list_page["next_page_css_selector"]).a["href"],
            )
        except Exception:
            return None

    def get_councillors(self):
        url = self.base_url
        while url:
            soup = self.get_page(url)

            url = self.get_next_link(soup)
            container = soup.select(self.list_page["container_css_selector"])[0]
            for councillor_html in container.select(
                self.list_page["councillor_css_selector"]
            ):
                yield councillor_html


class ModGovCouncillorScraper(BaseCouncillorScraper):
    class_tags = ["modgov"]
    ext = "xml"

    def run(self, run_log: RunLog):
        if self.options.get("aws_lambda"):
            self.delete_data_if_exists()
        else:
            self.clean_data_dir()
        wards = self.get_councillors()
        for ward in wards:
            for councillor_xml in ward.find_all("councillor"):
                try:
                    councillor = self.get_single_councillor(ward, councillor_xml)
                    self.process_councillor(councillor, councillor_xml)
                except SkipCouncillorException:
                    continue

        self.aws_tidy_up(run_log)

        self.report()

    def format_councillor_api_url(self):
        return "{}/mgWebService.asmx/GetCouncillorsByWard".format(self.base_url)

    def get_councillors(self):
        req = self.get(
            self.format_councillor_api_url(), extra_headers=self.extra_headers
        )
        req.raise_for_status()
        soup = BeautifulSoup(req.text, features="xml")
        return soup.findAll("ward")

    def get_single_councillor(self, ward, councillor_xml):
        identifier = councillor_xml.find("councillorid").text
        url = "{}/mgUserInfo.aspx?UID={}".format(self.base_url, identifier)
        name = councillor_xml.find("fullusername").text
        division = ward.find("wardtitle").text
        party = councillor_xml.find("politicalpartytitle").text

        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )

        # Emails
        with contextlib.suppress(AttributeError):
            councillor.email = councillor_xml.find("email").text

        # Photos
        with contextlib.suppress(AttributeError):
            councillor.photo_url = councillor_xml.photobigurl.text

        # Standing down
        IGNORED_ENDDATES = ["unspecified"]

        try:
            enddate = councillor_xml.find("termsofoffice").findAll("enddate")[-1].text
            if enddate not in IGNORED_ENDDATES:
                # councillor.standing_down = enddate
                standing_down = parse(enddate, dayfirst=True)
                councillor.standing_down = standing_down.isoformat()
        except AttributeError:
            pass

        if self.exclude_councillor_hook(councillor):
            raise SkipCouncillorException

        return councillor

    def exclude_councillor_hook(self, councillor: CouncillorBase):
        return False


class CMISCouncillorScraper(BaseCouncillorScraper):
    person_block_class_name = "PE_People_PersonBlock"
    division_text = "Ward:"
    class_tags = ["cmis"]

    def get_councillors(self):
        req = self.get(
            self.base_url,
            extra_headers=self.extra_headers,
        )
        soup = BeautifulSoup(req.text, "lxml")
        return soup.findAll("div", {"class": self.person_block_class_name})

    def get_party_name(self, list_page_html):
        return list_page_html.find_all("img")[-1]["title"].replace("(logo)", "").strip()

    def get_single_councillor(self, list_page_html):
        """
        Creates a candidate from a list page card.

        TODO: Pull more info
        """
        url = list_page_html.a["href"]
        identifier = url.split("/id/")[1].split("/")[0]
        name = list_page_html.find("div", {"class": "NameLink"}).getText(strip=True)
        if "Vacancy" in name:
            raise SkipCouncillorException("Vacancy")
        division = list_page_html.find(text=self.division_text).next.strip()
        party = self.get_party_name(list_page_html)

        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )

        req = self.get(url)
        soup = BeautifulSoup(req.text, "lxml")
        with contextlib.suppress(IndexError):
            councillor.email = soup.select(".Email")[0].getText(strip=True)
        return councillor


class JSONCouncillorScraper(BaseCouncillorScraper):
    pass
