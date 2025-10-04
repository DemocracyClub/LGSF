import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper

class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "#content",
        "councillor_css_selector": "tr",
    }

    def get_councillors(self):
        container = self.get_list_container()
        return container.select(self.list_page["councillor_css_selector"])[1:]

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.findAll("td")[0].a["href"])
        name = councillor_html.findAll("td")[0].get_text(strip=True)
        division = councillor_html.findAll("td")[1].get_text(strip=True)

        soup = self.get_page(url).select_one("table.memberDetails")
        party = (
            soup.find("td", text=re.compile("Party:"))
            .find_next("td")
            .get_text(strip=True)
        )

        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )

        councillor.email = soup.select("a[href^=mailto]")[0].get_text(strip=True)
        councillor.photo_url = urljoin(
            self.base_url,
            soup.select_one(".memberImage").select_one("img")["src"],
        )

        return councillor
