import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import (
    HTMLCouncillorScraper,
)


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".item-list--navigation",
        "councillor_css_selector": ".item-list__link",
    }

    def get_councillors(self):
        container = self.get_list_container()
        councillors = []
        for ward in container.select(".item-list__link"):
            ward_url = urljoin(self.base_url, ward["href"])
            ward_page = self.get_page(ward_url)
            councillors += ward_page.select(".item-list__link")
        return councillors

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html["href"])
        soup = self.get_page(url)
        name = (
            soup.find("dt", text=re.compile("Name"))
            .find_next_sibling("dd")
            .get_text(strip=True)
        )
        name = name.replace("Councillor ", "").strip()

        party = (
            soup.find("dt", text=re.compile("Political party"))
            .find_next_sibling("dd")
            .get_text(strip=True)
        )
        division = (
            soup.find("dt", text=re.compile("Ward"))
            .find_next_sibling("dd")
            .get_text(strip=True)
        )
        email = (
            soup.find("dt", text=re.compile("Ward"))
            .find_next_sibling("dd")
            .get_text(strip=True)
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )

        councillor.email = email
        photo = soup.select_one("dd img")
        if photo:
            photo_url = urljoin(self.base_url, photo["src"])
            councillor.photo_url = photo_url
        return councillor
