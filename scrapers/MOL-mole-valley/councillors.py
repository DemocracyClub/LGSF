import re
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.molevalley.gov.uk/councillors-decision-making/who-are-your-councillors/"
    verify_requests = False

    list_page = {
        "container_css_selector": ".contentcontainer",
        "councillor_css_selector": ".pt-cv-content-item",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = soup.select_one(".contentcontainer h2").get_text(strip=True).replace("Cllr ", "")

        if "Vacant" in name:
            raise SkipCouncillorException("Vacant")

        ward = councillor_html.select_one(".pt-cv-ctf-ward").get_text(strip=True)

        party = (
            soup.select(".contentcontainer p")[1]
            .get_text(strip=True)
            .replace(" Member", "")
            .strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = soup.select_one(
            ".bgcontainer a[href^=mailto]"
        ).get_text(strip=True)
        image = soup.select_one("article img")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
