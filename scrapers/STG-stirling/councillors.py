import json
import re
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper

class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "main",
        "councillor_css_selector": ".link-row__inner h3",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        schema_json = json.loads(
            soup.find("script", text=re.compile('''"@type": "Person"''')).contents[0]
        )

        name = schema_json["name"]
        if name == "Vacant":
            raise SkipCouncillorException

        party = schema_json["memberOf"]
        ward = (
            soup.select_one(".article-header__summary")
            .get_text(strip=True)
            .replace(party, "")
            .strip(",")
            .strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = soup.select_one("a[href^=mailto]")["href"].replace(
            "mailto:", ""
        )
        image = soup.select_one(".article-header__image img")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
