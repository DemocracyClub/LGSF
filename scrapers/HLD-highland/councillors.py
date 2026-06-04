import contextlib
import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "ul.list--listing",
        "councillor_css_selector": "li",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one("h1.page-heading")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )

        ward = (
            soup.find("strong", text=re.compile("Ward:"))
            .find_parent("p")
            .get_text(strip=True)
            .replace("Ward:", "")
            .strip()
        )
        ward = re.sub(r"[0-9]+ (.*)", r"\1", ward)

        party = (
            soup.find("strong", text=re.compile("Party:"))
            .find_parent("p")
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
            councillor.email = soup.select_one("a[href^=mailto]").get_text(strip=True)
        image = soup.select_one("img.image--feature")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
