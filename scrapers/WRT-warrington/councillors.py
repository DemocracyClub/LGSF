import contextlib
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".col-lg-9 > div.view-content",
        "councillor_css_selector": "div.views-row",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.findAll("a")[0]["href"])
        req = self.get(url)
        soup = BeautifulSoup(req.text, "lxml")
        name = soup.h1.get_text(strip=True)
        division = soup.select_one(".field--name-field-ward").get_text(strip=True)
        party = soup.select_one(".field--name-field-party-new").get_text(strip=True)

        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )
        with contextlib.suppress(AttributeError):
            councillor.email = (
                soup.select_one(".field--type-email").find("a").get_text(strip=True)
            )

        councillor.photo_url = "https:" + soup.img["src"]
        return councillor
