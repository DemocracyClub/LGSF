import re
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.rugby.gov.uk/councillors/"

    list_page = {
        "container_css_selector": ".card-page",
        "councillor_css_selector": "dd",
    }

    def get_single_councillor(self, councillor_html):
        if not councillor_html.contents:
            raise SkipCouncillorException()
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one(".councillor-details h3")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )

        if "vacant" in name.lower():
            raise SkipCouncillorException()

        ward = (
            soup.select_one("div.ward")
            .get_text(strip=True)
            .strip()
        )
        party = (
            soup.select_one("div.party")
            .get_text(strip=True)
            .strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = soup.find("span", text=re.compile("@")).get_text(strip=True)

        image = soup.select_one(".councillor-details img")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
