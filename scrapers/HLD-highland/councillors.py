import contextlib
import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper

class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "article.container ul.item-list__articles",
        "councillor_css_selector": "li",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one(".title h1").get_text(strip=True).replace("Councillor ", "")
        )

        ward = (
            soup.find("strong", text=re.compile("Ward:"))
            .find_parent("li")
            .get_text(strip=True)
            .replace("Ward:", "")
            .strip()
        )
        ward = re.sub(r"[0-9]+ (.*)", r"\1", ward)

        party = (
            soup.find("strong", text=re.compile("Party:"))
            .find_parent("li")
            .get_text(strip=True)
            .replace("Party:", "")
            .strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        with contextlib.suppress(AttributeError):
            councillor.email = soup.select_one("li a[href^=mailto]").get_text(
                strip=True
            )
        image = soup.select_one("article img")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
