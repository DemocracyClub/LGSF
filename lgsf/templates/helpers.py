from string import Template


class BaseTemplate:
    required_fields = []

    def __init__(self, context):
        self.context = context
        self.validate_context()

    def validate_context(self):
        for key in self.required_fields:
            assert key in self.context, "{} required in context".format(key)

    def format_template(self):
        teml = Template(self.template)
        return teml.substitute(**self.context)


class ModGovCouncillorTemplate(BaseTemplate):
    required_fields = ["base_url"]
    file_name = "councillors.py"

    template = """from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "$base_url"

    """


class CMISCouncillorTemplate(BaseTemplate):
    required_fields = ["base_url"]
    file_name = "councillors.py"

    template = """from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "$base_url"

    """


class HTMLCouncillorTemplate(BaseTemplate):
    required_fields = ["base_url"]
    file_name = "councillors.py"

    template = """from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "$base_url"
    list_page = {
        "container_css_selector": "EDITME",
        "councillor_css_selector": "EDITME",
    }

    def get_single_councillor(self, councillor_html):
        raise NotImplementedError
        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )
"""


class ModGovMinutesTemplate(BaseTemplate):
    required_fields = ["base_url"]
    file_name = "minutes.py"

    template = """from lgsf.minutes.scrapers import ModGovMinutesScraper


class Scraper(ModGovMinutesScraper):
    base_url = "$base_url"

    """


class CMISMinutesTemplate(BaseTemplate):
    required_fields = ["base_url"]
    file_name = "minutes.py"

    template = """from lgsf.minutes.scrapers import CMISMinutesScraper


class Scraper(CMISMinutesScraper):
    base_url = "$base_url"

    """


class CustomMinutesTemplate(BaseTemplate):
    """
    Starting point for a council whose meetings aren't on ModernGov or CMIS,
    where the scraper has to be written by hand.
    """

    required_fields = ["base_url"]
    file_name = "minutes.py"

    template = """from lgsf.minutes.scrapers import CustomHTMLMinutesScraper


class Scraper(CustomHTMLMinutesScraper):
    base_url = "$base_url"

    def get_meetings(self):
        # Return one item per meeting, in whatever shape suits this site:
        # a BeautifulSoup Tag, a dict from a JSON API, anything. Each item
        # is passed to get_single_meeting(). self.date_range gives the
        # window to filter to.
        soup = self.get_page(self.base_url)
        raise NotImplementedError
        return soup.select("EDITME")

    def get_single_meeting(self, meeting_data):
        # Turn one item from get_meetings() into a meeting. Raise
        # SkipMeetingException to drop an item, which is how you deal with
        # header rows or meetings belonging to a neighbouring council.
        raise NotImplementedError
        meeting = self.add_meeting(
            url,
            identifier=identifier,  # a stable id from the source, not a row number
            title=title,
            committee=committee,
            date=date,  # ISO format, e.g. "2026-08-19"
        )
        meeting.status = None
        meeting.location = None
        meeting.documents = [
            {
                "title": document_title,
                "url": document_url,
                "attachment_id": None,
                "category": self.categorise_document(document_title),
            }
        ]
        meeting.minutes_published = any(
            document["category"] == "minutes" for document in meeting.documents
        )
        return meeting
"""


TEMPLATES = {
    "councillor_scraper_modgov": ModGovCouncillorTemplate,
    "councillor_scraper_cmis": CMISCouncillorTemplate,
    "councillor_scraper_html": HTMLCouncillorTemplate,
    "minutes_scraper_modgov": ModGovMinutesTemplate,
    "minutes_scraper_cmis": CMISMinutesTemplate,
    "minutes_scraper_custom": CustomMinutesTemplate,
}
