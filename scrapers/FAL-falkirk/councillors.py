import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.falkirk.gov.uk/services/council-democracy/councillors-decision-making/councillors/"
    list_page = {
        "container_css_selector": "#page-body",
        "councillor_css_selector": ".list-group-item",
    }

    def _get_from_table(self, table, label):
        return (
            table.find("th", text=re.compile(label))
            .find_next("td")
            .get_text(strip=True)
        )

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url).select_one("#content")
        name = soup.h1.get_text(strip=True).replace("Councillor ", "")
        ward = self._get_from_table(soup, "Ward").split("(")[0]
        party = self._get_from_table(soup, "Party")

        councillor = self.add_councillor(
            url, identifier=url, name=name, division=ward, party=party
        )

        councillor.email = soup.select_one(".contact-email a[href^=mailto]").get_text(
            strip=True
        )
        councillor.photo_url = urljoin(url, soup.select_one("img")["src"])
        return councillor
