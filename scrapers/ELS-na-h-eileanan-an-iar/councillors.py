import contextlib
import re
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    verify_requests = False
    list_page = {
        "container_css_selector": "main",
        "councillor_css_selector": "h3",
    }

    def get_single_councillor(self, councillor_html):
        if "Related Documents" in councillor_html.get_text(strip=True):
            raise SkipCouncillorException()

        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = councillor_html.get_text(strip=True).replace("Councillor ", "")
        ward = (
            soup.find("h5", text=re.compile("Ward [0-9]+", re.I))
            .get_text(strip=True)
            .split(":")[-1]
            .strip()
        )

        # The website doesn't list party at all. This
        # is because most (maybe all but a couple) are
        # independent. This value is wrong, but more often right
        party = "Independent"

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )

        with contextlib.suppress(AttributeError):
            councillor.email = soup.select_one("h5 a[href^=mailto]").getText(strip=True)

        with contextlib.suppress(TypeError):
            councillor.photo_url = urljoin(
                self.base_url,
                soup.select_one("figure img")["src"],
            )

        return councillor
