import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.eastrenfrewshire.gov.uk/Find-my-councillor"

    list_page = {
        "container_css_selector": "main .a-body",
        "councillor_css_selector": "li .a-body__link",
    }

    def get_ward_from_person_url(self, url):
        ward = (
            self.base_url_soup.find("a", href=url)
            .find_previous("h2")
            .get_text(strip=True)
        )
        ward = re.sub(r"Ward [0-9]+ - (.*)", r"\1", ward)
        return ward

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one("h1.a-heading__title")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )
        ward = self.get_ward_from_person_url(url)
        party = soup.find("h2", text=re.compile("Party"))
        if not party:
            party = soup.find(text=re.compile("Party")).parent
        party = (
            party
            .find_next("p")
            .get_text(strip=True)
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )

        councillor.email = soup.select_one("td a[href^=mailto]").get_text(
            strip=True
        )
        councillor.photo_url = urljoin(
            self.base_url,
            soup.select_one(".a-relimage img")["src"],
        )
        return councillor
