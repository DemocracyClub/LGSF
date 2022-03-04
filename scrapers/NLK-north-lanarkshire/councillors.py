import re
from urllib.parse import urljoin

from councillors import SkipCouncillorException
from councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://mars.northlanarkshire.gov.uk/egenda/public/main.pl?op=ListCurrentMembers"
    list_page = {
        "container_css_selector": ".displayareacontainer",
        "councillor_css_selector": "tbody tr",
    }

    def extract_name(self, soup):
        """
        Not perfect, but a good enough way to get a full name from the
        odd way they have split the names up on the source website!
        """
        forenames_cell = soup.find("td", text="Forenames:")
        forenames = forenames_cell.find_next("td").get_text(strip=True)
        num_forenaems = len(forenames.split(" "))
        lastname = soup.select_one(".membername").get_text(strip=True)
        lastname = lastname.strip()[0:-num_forenaems]
        lastname = lastname.split("(")[0]
        full_name = f"{forenames} {lastname}".strip()
        return full_name

    def get_single_councillor(self, councillor_html):
        if councillor_html.th:
            # We need to skip header rows in tables
            raise SkipCouncillorException()

        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        ward = (
            soup.find("td", text=re.compile("Ward"))
            .find_next("td")
            .get_text(strip=True)
        )
        party = (
            soup.find("td", text=re.compile("Political Group"))
            .find_next("td")
            .get_text(strip=True)
        )

        name = self.extract_name(soup.select_one(".displayareacontainer"))
        councillor = self.add_councillor(
            url, name=name, division=ward, party=party, identifier=url
        )
        councillor.email = soup.select("a[href^=mailto]")[0].get_text(strip=True)
        councillor.photo_url = urljoin(
            self.base_url, soup.select_one("img.phototgraph")["src"]
        )

        return councillor
