import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.rossendale.gov.uk/councillors"

    list_page = {
        "container_css_selector": ".list--listing",
        "councillor_css_selector": "article",
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
        councillor.email = soup.select_one(".page-meta a[href^=mailto]")[
            "href"
        ].replace("mailto:", "")
        image = soup.select_one(".callout img")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
