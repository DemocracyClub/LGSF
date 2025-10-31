from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import (
    HTMLCouncillorScraper,
)


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".node-content",
        "councillor_css_selector": "tr",
    }

    def get_single_councillor(self, councillor_html):
        link = councillor_html.select_one("a")
        if not link:
            raise SkipCouncillorException
        url = urljoin(self.base_url, link["href"])
        soup = self.get_page(url)

        # H1 format: "Ward 4 - Coldside - Heather Anderson"
        # H2 has just the name
        h2 = soup.find("h2")
        if h2 and h2.strong:
            name = h2.strong.get_text(strip=True)
        else:
            # Fallback to old method if H2 not found
            heading = soup.h1.get_text(strip=True).split("-")
            name = heading[-1].strip()

        # Extract ward from H1
        h1_parts = soup.h1.get_text(strip=True).split("-")
        division = h1_parts[1].strip() if len(h1_parts) > 1 else ""

        party = councillor_html.find_all("td")[-1].string
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )
        email_el = soup.select("a[href^=mailto]")
        if email_el:
            councillor.email = email_el[0].get_text(strip=True)

        photo = soup.select_one("img.file-image")
        if not photo:
            # Try alternative selector for councillor images
            photo = soup.select_one('img[alt*="Councillor"]')
        if photo and photo.get("src"):
            councillor.photo_url = photo["src"]
        return councillor
