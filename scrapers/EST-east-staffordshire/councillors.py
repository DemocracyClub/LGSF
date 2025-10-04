import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".view-councillors",
        "councillor_css_selector": "h3",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url).select_one("main")

        name = councillor_html.get_text(strip=True).replace("Councillor ", "")
        division = (
            soup.find("div", text=re.compile("Ward representation"))
            .find_next("div", {"class": "field__item"})
            .get_text(strip=True)
        )
        party = (
            soup.find("div", text=re.compile("Party"))
            .find_next("div", {"class": "field__item"})
            .get_text(strip=True)
        )
        councillor = self.add_councillor(
            url, identifier=url, party=party, division=division, name=name
        )

        email_el = soup.find("div", text=re.compile("Email"))
        if email_el:
            councillor.email = email_el.find_next("div").get_text(strip=True)
        councillor.photo_url = soup.img["src"]

        return councillor
