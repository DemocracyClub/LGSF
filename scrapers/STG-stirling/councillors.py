import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.stirling.gov.uk/councillors"

    list_page = {
        "container_css_selector": ".page-content",
        "councillor_css_selector": ".councillor-card",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one(".container h1")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )

        ward = (
            soup.find("span", text=re.compile("Ward"))
            .find_parent("div")
            .get_text(strip=True)
            .strip()
        )

        party = (
            soup.find("span", text=re.compile("Party: .*"))
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
        councillor.email = soup.select_one(".email-address a[href^=mailto]")[
            "href"
        ].replace("mailto:", "")
        image = soup.select_one(".tab-content img")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
