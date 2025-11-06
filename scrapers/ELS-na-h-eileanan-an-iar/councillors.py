import contextlib
import re
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    verify_requests = False
    list_page = {
        "container_css_selector": "article",
        "councillor_css_selector": ".title-wrapper a",
    }

    def get_single_councillor(self, councillor_html):
        if "Related Documents" in councillor_html.get_text(strip=True):
            raise SkipCouncillorException()

        url = urljoin(self.base_url, councillor_html["href"])
        soup = self.get_page(url)

        name = councillor_html.get_text(strip=True).replace("Councillor ", "")
        ward = (
            soup.find("div", text=re.compile("Ward", re.I))
            .find_next("div")
            .get_text(strip=True)
        )

        party = (
            soup.find("div", text=re.compile("Party", re.I))
            .find_next("div")
            .get_text(strip=True)
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )

        with contextlib.suppress(AttributeError):
            councillor.email = soup.select_one(".banner-wrapper a[href^=mailto]").getText(strip=True)

        with contextlib.suppress(TypeError):
            councillor.photo_url = urljoin(
                self.base_url,
                soup.select_one(".banner-wrapper img")["src"],
            )

        return councillor
