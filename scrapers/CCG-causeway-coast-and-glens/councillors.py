import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "#board",
        "councillor_css_selector": "a.no-underline",
    }

    def get_single_councillor(self, councillor_html):
        councillor_url = councillor_html["href"]
        url = urljoin(self.base_url, councillor_url)
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True)

        party = (
            soup.find("span", text=re.compile("Party:"))
            .find_parent("p")
            .get_text(strip=True)
            .replace("Party:", "")
        )
        division = (
            soup.find("span", text=re.compile("District Electoral Area:"))
            .find_parent("p")
            .get_text(strip=True)
            .replace("District Electoral Area:", "")
        )

        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )
        container = soup.select_one("main")

        councillor.email = container.select_one("a[href^=mailto]")["href"].replace(
            "mailto:", ""
        )

        councillor.photo_url = container.select_one("img")["src"]

        return councillor
