import re

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://stroud.moderngov.co.uk"
