import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://forms.wyreforestdc.gov.uk/council/committees/com55.htm"
    list_page = {
        "container_css_selector": ".container table",
        "councillor_css_selector": "td",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        req = requests.get(url)
        soup = BeautifulSoup(req.text, "lxml")

        name = re.sub(
            "[\s]+", " ", soup.findAll("td")[0].td.get_text(strip=True)
        )
        party = soup.findAll("td")[3].findAll("a")[0].get_text(strip=True)
        division = soup.findAll("td")[3].findAll("a")[1].get_text(strip=True)

        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        councillor.photo_url = (
            "http://www.wyreforest.gov.uk/council/councillors/"
            + soup.findAll("td")[2].img["src"]
        )
        councillor.email = (
            soup.findAll("td")[5].findAll("a")[0].get_text(strip=True)
        )
        return councillor
