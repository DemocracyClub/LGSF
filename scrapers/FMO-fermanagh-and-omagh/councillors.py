import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper

class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".page__content__wrapper__inner",
        "councillor_css_selector": ".card",
    }

    def get_single_councillor(self, councillor_html):
        councillor_url = councillor_html.select_one("a")["href"]
        url = urljoin(self.base_url, councillor_url)
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True)

        division = (
            soup.find("th", text=re.compile("Ward"))
            .find_next("td")
            .get_text(strip=True)
        )

        party = (
            soup.find("th", text=re.compile("Party"))
            .find_next("td")
            .get_text(strip=True)
        )
        councillor = self.add_councillor(
            url=url, identifier=url, name=name, division=division, party=party
        )

        councillor.email = (
            soup.find("th", text=re.compile("Email"))
            .find_next("td")
            .get_text(strip=True)
        )

        councillor.photo_url = soup.select_one(".person-avatar")["src"]

        return councillor
