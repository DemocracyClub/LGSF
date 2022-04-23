import re
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.southnorfolkandbroadland.gov.uk/directory/3/south-norfolk-councillor-directory/category/11"
    list_page = {
        "container_css_selector": "ul.list--record",
        "councillor_css_selector": ".list__item",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(
            self.base_url,
            councillor_html.a["href"],
        )
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True)
        if name == "Vacancy":
            raise SkipCouncillorException("Vacancy")
        names = name.split(", ")
        name = f"{names[1]} {names[0]}"
        party = soup.find(text=re.compile("Party")).findNext("dd").get_text(strip=True)
        division = (
            soup.find(text=re.compile("Ward")).findNext("dd").get_text(strip=True)
        )

        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        councillor.email = soup.select("a[href^=mailto]")[0].get_text(strip=True)

        councillor.photo_url = urljoin(
            "https://www.south-norfolk.gov.uk",
            soup.select_one(".definition__content--image").img["src"],
        )
        return councillor
