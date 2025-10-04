import contextlib
import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "ul.list--political",
        "councillor_css_selector": ".list__item",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one("h1.page-heading")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )
        callout = soup.select(".callout__list .list__item")
        for element in callout:
            text = element.get_text(strip=True)
            if text.startswith("Ward:"):
                ward = text.replace("Ward:", "").strip()
                ward = re.sub(r"[0-9]+ (.*)", r"\1", ward)
            if text.startswith("Party:"):
                party = text.replace("Party:", "").strip()

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = soup.select_one(".listing__summary a[href^=mailto]").getText(
            strip=True
        )
        with contextlib.suppress(TypeError):
            councillor.photo_url = urljoin(
                self.base_url,
                soup.select_one(".listing--with-image img.listing__image")["src"],
            )

        return councillor
