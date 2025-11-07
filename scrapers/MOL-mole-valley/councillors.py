from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    verify_requests = False

    list_page = {
        "container_css_selector": ".contentcontainer",
        "councillor_css_selector": ".pt-cv-content-item",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one(".contentcontainer h2")
            .get_text(strip=True)
            .replace("Councillor ", "")
            .replace("Cllr ", "")
            .replace("Cllr", "")  # Handle cases without space
            .strip()
        )

        if "Vacant" in name:
            raise SkipCouncillorException("Vacant")

        ward = councillor_html.select_one(".pt-cv-ctf-ward").get_text(strip=True)

        # Party is in the second strong tag (after ward)
        party = (
            soup.select("strong")[1].get_text(strip=True).replace(" Member", "").strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        # Email is optional
        email_element = soup.select_one(".bgcontainer a[href^=mailto]")
        if email_element:
            councillor.email = email_element.get_text(strip=True)

        # Photo is in contentcontainer, check data-src for lazy-loaded images
        image = soup.select_one(".contentcontainer img")
        if image:
            # Check for lazy-loaded image (data-src) first, fallback to src
            img_src = image.get("data-src") or image.get("src")
            if img_src and not img_src.startswith("data:"):
                councillor.photo_url = urljoin(self.base_url, img_src)
        return councillor
