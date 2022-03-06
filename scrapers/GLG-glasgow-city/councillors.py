import re
from urllib.parse import urljoin

from lgsf.councillors.exceptions import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.glasgow.gov.uk/councillorsandcommittees/allMembers.asp?sort=0&page=0&rec=100"
    list_page = {
        "container_css_selector": ".resultsNew",
        "councillor_css_selector": "tr",
    }

    def get_single_councillor(self, councillor_html):
        if councillor_html.attrs.get("class") == ["resultsHeader"]:
            raise SkipCouncillorException
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.find("td", text=re.compile("Title:"))
            .find_next("td")
            .get_text(strip=True)
        )
        name = name.replace("Councillor ", "")

        ward = (
            soup.find("td", text=re.compile("Ward:"))
            .find_next("td")
            .get_text(strip=True)
        )
        ward = ward.split("(")[0]

        party = (
            soup.find("td", text=re.compile("Party:"))
            .find_next("td")
            .get_text(strip=True)
        )

        councillor = self.add_councillor(
            url=url, identifier=url, name=name, division=ward, party=party
        )

        councillor.email = soup.select_one(
            ".memberDetailsContact a[href^=mailto]"
        ).get_text(strip=True)

        councillor.photo_url = urljoin(
            self.base_url, soup.select_one(".memberDetailsInfo img")["src"]
        )

        return councillor
