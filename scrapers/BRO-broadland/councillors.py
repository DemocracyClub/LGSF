import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.southnorfolkandbroadland.gov.uk/directory/2/broadland-councillor-directory/category/10"

    list_page = {
        "container_css_selector": ".list--record",
        "councillor_css_selector": ".list__link",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html["href"])
        soup = self.get_page(url)

        name = soup.select_one("h1").get_text(strip=True).replace("Councillor ", "")
        name = " ".join(name.split(" ")[1::-1]).replace(",", "")
        party = (
            soup.find("dt", text=re.compile("Party"))
            .find_next("dd")
            .get_text(strip=True)
        )
        division = (
            soup.find("dt", text=re.compile("Ward"))
            .find_next("dd")
            .get_text(strip=True)
        )

        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        email = (
            soup.find("dt", text=re.compile("Email"))
            .find_next("dd")
            .get_text(strip=True)
        )
        councillor.email = email
        councillor.photo_url = urljoin(
            soup.base_url, soup.select_one(".directory__image")["src"]
        )
        return councillor
