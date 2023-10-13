import re

from lgsf.councillors.scrapers import HTMLCouncillorScraper, ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://democracy.swdevon.gov.uk/"
