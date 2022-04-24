import re
from urllib.parse import urljoin

from councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.molevalley.gov.uk/home/council/councillors/who-are-your-councillors"

    list_page = {
        "container_css_selector": "table.w3-table-all",
        "councillor_css_selector": "tr",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = soup.select_one("h1").get_text(strip=True).replace("Cllr ", "")

        ward = (
            soup.find("label", text=re.compile("Ward"))
            .find_next("div")
            .get_text(strip=True)
            .strip()
            .title()
        )

        party = (
            soup.select_one(".field--name-field-political-party")
            .get_text(strip=True)
            .replace(" Member", "")
            .strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = soup.select_one(
            ".field--name-field-contact-email a[href^=mailto]"
        ).get_text(strip=True)
        image = soup.select_one("article img")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
