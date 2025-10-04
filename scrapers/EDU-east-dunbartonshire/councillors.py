import re
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "div.view-content",
        "councillor_css_selector": ".views-row",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one(".main-container h1")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )
        intro = soup.find("p", text=re.compile("is one of East Dunbartonshire Council"))
        if not intro:
            raise SkipCouncillorException()
        ward = (
            intro.get_text(strip=True)
            .split("Elected Members for Ward ")[1]
            .split(" and ")[0]
            .strip()
        )
        ward = re.sub(r"[0-9]+ - (.*)(\.)?", r"\1", ward)
        party = councillor_html.get_text(strip=True)

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = soup.select_one(
            ".field-name-field-email-contact a[href^=mailto]"
        ).getText(strip=True)
        councillor.photo_url = urljoin(
            self.base_url,
            soup.select_one(".field-name-field-picture img.img-responsive")["src"],
        )
        return councillor
