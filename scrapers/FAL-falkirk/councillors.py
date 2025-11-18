import contextlib
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "main",
        "councillor_css_selector": "a[href^='/councillor/']",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html["href"])
        soup = self.get_page(url)

        # Name is in h1, remove "Councillor " prefix and other titles
        name = (
            soup.select_one("main h1")
            .get_text(strip=True)
            .replace("Councillor ", "")
            .replace("Provost ", "")
            .replace("Depute Provost ", "")
            .replace("Baillie ", "")
            .strip()
        )

        # Ward is in div.fs-5.mb-2 after h1
        ward = soup.select_one("main div.fs-5.mb-2").get_text(strip=True)

        # Party is the first td in the table
        party = soup.select_one("table td").get_text(strip=True)

        councillor = self.add_councillor(
            url, identifier=url, name=name, division=ward, party=party
        )

        # Email
        with contextlib.suppress(AttributeError):
            email_link = soup.select_one("a[href^=mailto]")
            if email_link:
                councillor.email = email_link.get_text(strip=True)

        # Photo - use API endpoint instead of base64 data URL
        # Extract councillor ID from URL (e.g., /councillor/891 -> 891)
        councillor_id = url.rstrip("/").split("/")[-1]
        councillor.photo_url = f"{self.base_url}/api/councillors/photo/{councillor_id}"

        return councillor
