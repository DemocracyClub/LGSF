import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://antrimandnewtownabbey.gov.uk/councillors/"
    list_page = {
        "container_css_selector": "main",
        "councillor_css_selector": ".contact-card",
    }

    raw_html = None

    def get_raw_html(self):
        if not self.raw_html:
            self.raw_html = self.get_page(self.base_url)
        return self.raw_html

    def get_ward_for_person(self, name):
        raw_html = self.get_raw_html()
        title_tag = raw_html.find(string=re.compile(name))
        ward = title_tag.find_all_previous("div", {"class": re.compile("wrapper-*")})[
            0
        ].h2.get_text(strip=True)
        return ward.replace(" Councillors", "").strip()

    def get_single_councillor(self, councillor_html):
        image_style = councillor_html.select("div.img")[0]["style"]
        image_url = image_style.split("'")[1].split("?")[0]
        image_url = urljoin(self.base_url, image_url)
        url = image_url

        name = councillor_html.select("p.title")[0].get_text(strip=True)
        party = councillor_html.select("p.title span")[0].get_text(strip=True)
        name = name.replace(party, "")
        division = self.get_ward_for_person(name)

        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        councillor.email = councillor.email = councillor_html.select("a[href^=mailto]")[
            0
        ]["href"].split(":")[1]
        councillor.photo_url = image_url
        return councillor
