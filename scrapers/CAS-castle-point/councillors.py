import re

from lgsf.councillors.scrapers import HTMLCouncillorScraper, CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://castlepoint.cmis.uk.com/castlepoint/Councillors.aspx"
