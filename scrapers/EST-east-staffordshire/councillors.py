import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):

    base_url = "http://eaststaffsbc.gov.uk/council-democracy/councillors"
    list_page = {
        "container_css_selector": ".view-councillors",
        "councillor_css_selector": "h3",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url).select_one("#content")

        name = soup.h1.get_text(strip=True)
        division = soup.find("h3", text=re.compile("Ward representation")).find_next("li").get_text(strip=True)
        party = soup.find("h3", text=re.compile("Party")).find_next("li").get_text(strip=True)
        councillor = self.add_councillor(url, identifier=url, party=party, division=division, name=name)

        councillor.email = soup.find("div", text=re.compile("Email")).find_next("div").get_text(strip=True)
        councillor.photo_url = soup.img["src"]

        return councillor
