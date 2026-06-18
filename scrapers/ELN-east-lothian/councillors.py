import contextlib
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".view-id-councillors",
        "councillor_css_selector": ".card",
    }

    def get_single_councillor(self, councillor_html):
        link_el = councillor_html.select_one(".views-field-title a")
        url = urljoin(self.base_url, link_el["href"])
        name = link_el.get_text(strip=True)

        ward_el = councillor_html.select_one(".views-field-field-ward .field-content")
        ward = ward_el.get_text(strip=True) if ward_el else ""

        party_el = councillor_html.select_one(".views-field-field-party .field-content")
        party = party_el.get_text(strip=True) if party_el else ""

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )

        soup = self.get_page(url)
        with contextlib.suppress(AttributeError):
            councillor.email = soup.select_one("a[href^=mailto]").get_text(strip=True)

        img = councillor_html.select_one(".views-field-field-image img")
        if img:
            img_src = img.get("data-src") or img.get("src")
            if img_src and not img_src.startswith("data:"):
                councillor.photo_url = urljoin(self.base_url, img_src)

        return councillor
