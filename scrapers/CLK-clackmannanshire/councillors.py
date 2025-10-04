import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "div.col-md-14",
        "councillor_css_selector": ".col-sm-3",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)
        name = (
            soup.select_one("div.col-md-14")
            .h2.get_text(strip=True)
            .replace("Councillor", "")
        )
        ward = (
            soup.find("strong", text=re.compile("Ward:"))
            .find_parent("li")
            .get_text(strip=True)
            .replace("Ward:", "")
            .strip()
        )
        party = councillor_html.h5.get_text(strip=True)

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
