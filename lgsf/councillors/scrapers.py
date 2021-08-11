import abc

from bs4 import BeautifulSoup
from dateutil.parser import parse

from lgsf.scrapers import ScraperBase, CodeCommitMixin
from lgsf.councillors import CouncillorBase, json


class BaseCouncillorScraper(CodeCommitMixin, ScraperBase):
    tags = []
    class_tags = []
    ext = "html"
    verify_requests = True

    def __init__(self, options, console):
        super().__init__(options, console)
        self.councillors = set()
        self.repository = "CouncillorsRepo"

    @abc.abstractmethod
    def get_councillors(self):
        pass

    @abc.abstractmethod
    def get_single_councillor(self):
        pass

    def add_councillor(self, url, **kwargs):
        councillor = CouncillorBase(url, **kwargs)
        self.councillors.add(councillor)
        return councillor

    @property
    def get_tags(self):
        return self.tags + self.class_tags

    def run(self):

        if self.options.get("aws_lambda"):
            self.delete_data_if_exists()

        for councillor_html in self.get_councillors():
            councillor = self.get_single_councillor(councillor_html)
            self.process_councillor(councillor, councillor_html)

        self.aws_tidy_up()

        self.report()

    def process_councillor(self, councillor, councillor_raw_str):
        if self.options.get("aws_lambda"):
            # stage...
            self.stage_councillor(councillor_raw_str, councillor)

            # Do a batch commit if needed...
            if len(self.put_files) > 90:
                self.process_batch()
        else:
            self.save_councillor(councillor_raw_str, councillor)

    def stage_councillor(self, councillor_html, councillor):
        council = self.options["council"]
        json_file_path = f"{council}/json/{councillor.as_file_name()}.json"
        raw_file_path = f"{council}/raw/{councillor.as_file_name()}.html"
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
                    "fileContent": bytes(councillor_html.prettify(), "utf-8"),
                },
            ]
        )

    def save_councillor(self, raw_content, councillor_obj):
        assert (
            type(councillor_obj) == CouncillorBase
        ), "Scrapers must return a councillor object"
        file_name = "{}.{}".format(councillor_obj.as_file_name(), self.ext)
        self.save_raw(file_name, raw_content.prettify())
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
        page = self.get(url).text
        return BeautifulSoup(page, "html5lib")

    def get_list_container(self):
        """
        Uses :func:`ScraperBase.get <ScraperBase.get>` to request the
        `base_url` and selects the HTML using the selector defined in
        `list_page['container_css_selector']`

        :return: A :class:`BeautifulSoup` object


        .. todo::
            raise if more than one node found
        """
        soup = self.get_page(self.base_url)
        return soup.select(self.list_page["container_css_selector"])[0]

    def get_councillors(self):
        container = self.get_list_container()
        return container.select(self.list_page["councillor_css_selector"])


class PagedHTMLCouncillorScraper(HTMLCouncillorScraper):
    def get_next_link(self, soup):
        try:
            return soup.select(self.list_page["next_page_css_selector"])[0].a["href"]
        except:
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

    def run(self):

        if self.options.get("aws_lambda"):
            self.delete_data_if_exists()
        wards = self.get_councillors()
        for ward in wards:
            for councillor_xml in ward.find_all("councillor"):
                councillor = self.get_single_councillor(ward, councillor_xml)
                self.process_councillor(councillor, councillor_xml)

        self.aws_tidy_up()
        self.report()

    def format_councillor_api_url(self):
        return "{}/mgWebService.asmx/GetCouncillorsByWard".format(self.base_url)

    def get_councillors(self):
        req = self.get(self.format_councillor_api_url(), verify=self.verify_requests)
        soup = BeautifulSoup(req.text, "lxml")
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
        try:
            councillor.email = councillor_xml.find("email").text
        except AttributeError:
            pass

        # Photos
        try:
            councillor.photo_url = councillor_xml.photobigurl.text
        except AttributeError:
            pass

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

        return councillor


class CMISCouncillorScraper(BaseCouncillorScraper):
    person_block_class_name = "PE_People_PersonBlock"
    division_text = "Ward:"
    class_tags = ["cmis"]

    def get_councillors(self):
        req = self.get(self.base_url)
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
        try:
            councillor.email = soup.select(".Email")[0].getText(strip=True)
        except IndexError:
            # Can't find an email, just ignore it
            pass
        return councillor
