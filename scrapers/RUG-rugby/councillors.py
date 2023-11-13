import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.rugby.gov.uk/councillors/name"

    list_page = {
        "container_css_selector": "article ul.councillors",
        "councillor_css_selector": "li",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one("article h1")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )

        ward = (
            soup.find("strong", text=re.compile("Ward:"))
            .find_parent("li")
            .get_text(strip=True)
            .replace("Ward:", "")
            .strip()
        )

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
        councillor.email = soup.select_one(".callout a[href^=mailto]")[
            "href"
        ].replace("mailto:", "")
        image = soup.select_one(".callout img")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
