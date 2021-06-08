from string import Template


class BaseTemplate:
    required_fields = []

    def __init__(self, context):
        self.context = context
        self.validate_context()

    def validate_context(self):
        for key in self.required_fields:
            assert key in self.context.keys(), "{} required in context".format(key)

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

from lgsf.scrapers.councillors import HTMLCouncillorScraper


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


TEMPLATES = {
    "councillor_scraper_modgov": ModGovCouncillorTemplate,
    "councillor_scraper_cmis": CMISCouncillorTemplate,
    "councillor_scraper_html": HTMLCouncillorTemplate,
}
