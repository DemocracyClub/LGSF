import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.cne-siar.gov.uk/your-council/wards-and-councillors/council-members/"

    list_page = {
        "container_css_selector": "article .row-fluid",
        "councillor_css_selector": ".cnes_listitem",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one("h1.cnes_pagetitle")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )

        ward = (
            soup.find("p", text=re.compile("Ward:")).find_next("p").get_text(strip=True)
        )

        party_row = soup.find("p", text=re.compile("Party:"))
        if party_row:
            party = party_row.find_next("p").get_text(strip=True)
        else:
            party = "Independent"

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = soup.select_one("article a[href^=mailto]").getText(
            strip=True
        )
        try:
            councillor.photo_url = urljoin(
                self.base_url,
                soup.select_one("article img")["src"],
            )
        except TypeError:
            pass
        return councillor
