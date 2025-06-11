from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import (
    HTMLCouncillorScraper,
)


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.dundeecity.gov.uk/service-area/councillors/councillors-political-wards-ward-number"
    list_page = {
        "container_css_selector": ".node-content",
        "councillor_css_selector": "tr",
    }

    def get_single_councillor(self, councillor_html):
        link = councillor_html.select_one("a")
        if not link:
            raise SkipCouncillorException
        url = urljoin(self.base_url, link["href"])
        soup = self.get_page(url)
        heading = soup.h1.string.split("-")
        name = heading[-1]

        division = heading[1]
        party = councillor_html.find_all("td")[-1].string
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )
        email_el = soup.select("a[href^=mailto]")
        if email_el:
            councillor.email = email_el[0].get_text(strip=True)

        councillor.photo_url = soup.select_one("img.file-image ")["src"]
        return councillor
