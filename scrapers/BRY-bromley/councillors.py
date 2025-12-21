import contextlib
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "div.mgContent",
        "councillor_css_selector": "li",
    }

    def get_single_councillor(self, councillor_html):
        # Get the councillor link
        link = councillor_html.select_one("a")
        if not link:
            from lgsf.councillors import SkipCouncillorException

            raise SkipCouncillorException("No link found")

        url = urljoin(self.base_url, link["href"])

        # Visit the detail page
        soup = self.get_page(url)

        # Extract name (remove "Councillor " prefix)
        # There are 2 h1s - we want the second one inside page-content
        name = (
            soup.select_one("div.page-content h1")
            .get_text(strip=True)
            .replace("Councillor ", "")
            .strip()
        )

        # Extract party and ward from the page-content div
        # Party and ward are in paragraphs like "Party: Labour" and "Ward: Clock House"
        page_content = soup.select_one("div.page-content")

        party = None
        division = None
        for para in page_content.find_all("p"):
            text = para.get_text(strip=True)
            if text.startswith("Party:"):
                party = text.replace("Party:", "").strip()
            elif text.startswith("Ward:"):
                division = text.replace("Ward:", "").strip()

        # Create councillor object
        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )

        # Extract email (optional)
        with contextlib.suppress(AttributeError, IndexError):
            email_link = soup.select_one("a[href^=mailto]")
            if email_link:
                councillor.email = email_link["href"].split(":", 1)[1]

        # Extract photo (optional)
        with contextlib.suppress(AttributeError, TypeError):
            photo = soup.select_one("img[alt*='Profile image']")
            if photo and photo.get("src"):
                photo_src = photo["src"]
                if not photo_src.startswith("data:"):
                    councillor.photo_url = urljoin(self.base_url, photo_src)

        return councillor
