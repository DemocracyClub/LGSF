import abc

from bs4 import BeautifulSoup
from dateutil.parser import parse

from lgsf.scrapers import ScraperBase
from lgsf.councillors import CouncillorBase


class BaseCouncillorScraper(ScraperBase):
    tags = []
    class_tags = []
    ext = "html"

    def __init__(self, options):
        super().__init__(options)
        self.councillors = set()

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

        for councillor_html in self.get_councillors():
            councillor = self.get_single_councillor(councillor_html)
            self.save_councillor(councillor_html, councillor)
        self.report()

    def save_councillor(self, raw_content, councillor_obj):
        assert type(councillor_obj) == CouncillorBase
        file_name = "{}.{}".format(councillor_obj.as_file_name(), self.ext)
        self.save_raw(file_name, raw_content.prettify())
        self.save_json(councillor_obj)

    def report(self):
        if self.options.get("verbose"):
            print("Found {} councillors".format(len(self.councillors)))


class HTMLCouncillorScraper(BaseCouncillorScraper):
    class_tags = ["html"]

    def get_list_container(self):
        """
        Uses :func:`ScraperBase.get <ScraperBase.get>` to request the
        `base_url` and selects the HTML using the selector defined in
        `list_page['container_css_selector']`

        :return: A :class:`BeautifulSoup` object


        .. todo::
            raise if more than one node found
        """
        page = self.get(self.base_url).text
        soup = BeautifulSoup(page, "html5lib")
        return soup.select(self.list_page["container_css_selector"])[0]

    def get_councillors(self):
        container = self.get_list_container()
        return container.select(self.list_page["councillor_css_selector"])


class ModGovCouncillorScraper(BaseCouncillorScraper):
    class_tags = ["modgov"]
    ext = "xml"

    def run(self):
        wards = self.get_councillors()

        for ward in wards:
            for councillor_xml in ward.find_all("councillor"):
                councillor = self.get_single_councillor(ward, councillor_xml)
                self.save_councillor(councillor_xml, councillor)
        self.report()

    def format_councillor_api_url(self):
        return "{}/mgWebService.asmx/GetCouncillorsByWard".format(self.base_url)

    def get_councillors(self):
        req = self.get(self.format_councillor_api_url())
        soup = BeautifulSoup(req.text, "lxml")
        return soup.findAll("wards")

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
            enddate = (
                councillor_xml.find("termsofoffice").findAll("enddate")[-1].text
            )
            if enddate not in IGNORED_ENDDATES:
                # councillor.standing_down = enddate
                standing_down = parse(enddate, dayfirst=True)
                councillor.standing_down = standing_down.isoformat()
        except AttributeError:
            pass

        return councillor


class CMISCouncillorScraper(BaseCouncillorScraper):
    person_block_class_name = "PE_People_PersonBlock"
    class_tags = ["cmis"]

    def get_councillors(self):
        req = self.get(self.base_url)
        soup = BeautifulSoup(req.text, "lxml")
        return soup.findAll("div", {"class": self.person_block_class_name})

    def get_party_name(self, list_page_html):
        return (
            list_page_html.find_all("img")[-1]["title"]
            .replace("(logo)", "")
            .strip()
        )

    def get_single_councillor(self, list_page_html):
        """
        Creates a candidate from a list page card.

        TODO: Pull more info
        """
        url = list_page_html.a["href"]
        identifier = url.split("/id/")[1].split("/")[0]
        name = list_page_html.find("div", {"class": "NameLink"}).getText(
            strip=True
        )
        division = list_page_html.find(text="Ward:").next.strip()
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
