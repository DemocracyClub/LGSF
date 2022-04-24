import re
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://derbyshiredales.gov.uk/your-council/councillors"

    list_page = {
        "container_css_selector": "div.fulllist",
        "councillor_css_selector": ".blog-item__panel",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        if "contact-your-councillor" in url:
            raise SkipCouncillorException()
        soup = self.get_page(url)

        name = soup.select_one("article h1").get_text(strip=True)
        ward = (
            soup.h1.find_next("p")
            .get_text(strip=True)
            .split("Ward:")[1]
            .split("(")[0]
            .strip()
        )
        party = (
            soup.find(text=re.compile("Party|Group"))
            .find_parent("p")
            .get_text(strip=True)
            .replace("Party:", "")
            .replace("Group:", "")
            .strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = soup.select_one("a[href^=mailto]").getText(strip=True)
        if soup.select_one(".page-image img"):
            councillor.photo_url = urljoin(
                self.base_url, soup.select_one(".page-image img")["src"]
            )
        return councillor
