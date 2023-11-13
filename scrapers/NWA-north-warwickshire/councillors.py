import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.northwarks.gov.uk/councillors/name"

    list_page = {
        "container_css_selector": ".item-list--councillors",
        "councillor_css_selector": ".councillor",
    }

    def get_single_councillor(self, councillor_html):
        url = councillor_html.a["href"]
        name = councillor_html.h3.get_text(strip=True)

        division = (
            councillor_html.find("strong", text=re.compile("Ward:"))
            .find_parent("li")
            .get_text(strip=True)
            .replace("Ward:", "")
            .strip()
        )
        party = (
            councillor_html.find("strong", text=re.compile("Party:"))
            .find_parent("li")
            .get_text(strip=True)
            .replace("Party:", "")
            .strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )

        soup = self.get_page(url)

        councillor.email = soup.select_one(
            ".callout--councillor a[href^=mailto]"
        )["href"].replace("mailto:", "")
        councillor.photo_url = urljoin(
            self.base_url, councillor_html.img["src"]
        )
        return councillor
