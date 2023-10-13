import re
from urllib.parse import urljoin

from lgsf.councillors.exceptions import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper, CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://northlanarkshire.cmis.uk.com/Councillors.aspx"
