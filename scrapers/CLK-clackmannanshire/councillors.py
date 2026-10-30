import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "main .ds_layout__content",
        "councillor_css_selector": ".ds_layout__item",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url).select_one("main .ds_layout__content")
        name = soup.select_one("h1 .fn").get_text(strip=True)
        ward = (
            soup.find("strong", string=re.compile("Ward:"))
            .find_parent("li")
            .get_text(strip=True)
            .replace("Ward:", "")
            .strip()
        )
        party = councillor_html.h3.get_text(strip=True)

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = soup.select_one("a.email").getText(strip=True)
        if soup.select_one("img.right"):
            councillor.photo_url = urljoin(
                self.base_url, soup.select_one("img.right")["src"]
            )
        return councillor
