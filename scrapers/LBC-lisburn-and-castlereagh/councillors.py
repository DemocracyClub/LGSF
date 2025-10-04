import re
import time
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "#main-content",
        "councillor_css_selector": ".councillor-card",
    }

    def get_single_councillor(self, councillor_html):
        councillor_url = councillor_html.select_one("a")["href"]
        url = urljoin(self.base_url, councillor_url)
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True)
        try:
            party = (
                soup.find("dt", text=re.compile("Political Party"))
                .find_next("dd")
                .get_text(strip=True)
            )
        except AttributeError:
            party = "Independent"
        division = (
            soup.find("dt", text=re.compile("District Electoral Area"))
            .find_next("dd")
            .get_text(strip=True)
        )

        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )
        container = soup.select_one(".journal-content-article ")

        councillor.email = container.select_one("a[href^=mailto]")["href"].replace(
            "mailto:", ""
        )

        councillor.photo_url = container.select_one("img")["src"]

        # Page is rate limited! Sleep between each request
        time.sleep(10)

        return councillor
