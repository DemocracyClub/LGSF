import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper

class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".table-responsive",
        "councillor_css_selector": "tbody tr",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.select_one("a")["href"])
        soup = self.get_page(url)
        name = (
            soup.select_one("h1.page-header")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )

        ward = (
            soup.find("td", text=re.compile("Ward"))
            .find_next("td")
            .get_text(strip=True)
        )

        party = councillor_html.select("td")[4].get_text(strip=True)

        councillor = self.add_councillor(
            url, name=name, division=ward, party=party, identifier=url
        )
        councillor.email = soup.select("a[href^=mailto]")[0].get_text(strip=True)
        councillor.photo_url = urljoin(
            self.base_url, soup.select_one("img.img-responsive")["src"]
        )

        return councillor
