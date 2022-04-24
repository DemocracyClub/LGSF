import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.inverclyde.gov.uk/meetings/councillors"

    list_page = {
        "container_css_selector": "div#listing",
        "councillor_css_selector": ".cardWrap",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one("article h1")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )
        last, first = name.split(", ")
        name = f"{first} {last}"

        ward = (
            soup.find("span", text=re.compile("Ward:"))
            .find_next("span")
            .get_text(strip=True)
            .strip()
        )
        ward = re.sub(r"Ward [0-9]+ - (.*)", r"\1", ward)

        party = (
            soup.find("span", text=re.compile("Party:"))
            .find_next("span")
            .get_text(strip=True)
            .strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = soup.select_one(".panel a[href^=mailto]").get_text(
            strip=True
        )
        image = soup.select_one(".contentImg img")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
