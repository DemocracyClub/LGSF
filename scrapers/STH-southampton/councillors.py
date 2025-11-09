import contextlib
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    http_lib = "requests"

    list_page = {
        "container_css_selector": "div.mgThumbsList",
        "councillor_css_selector": "li",
    }

    def get_list_container(self):
        # Override to use the full councillor index URL instead of base_url
        url = "https://www.southampton.gov.uk/modernGov/mgMemberIndex.aspx"
        self.base_url_soup = self.get_page(url)
        container = self.base_url_soup.select_one(
            self.list_page["container_css_selector"]
        )
        return container

    def get_single_councillor(self, councillor_html):
        # Skip if no link (shouldn't happen but safety check)
        link = councillor_html.find("a")
        if not link:
            raise SkipCouncillorException("No link found")

        # Construct full URL from the listing page URL
        list_url = "https://www.southampton.gov.uk/modernGov/mgMemberIndex.aspx"
        url = urljoin(list_url, link["href"])

        # Visit individual councillor page
        soup = self.get_page(url)

        # Extract name from the link text, removing "Councillor " prefix
        name = (
            link.get_text(strip=True)
            .replace("Councillor ", "")
            .replace("Cllr ", "")
            .strip()
        )

        # Extract ward and party from paragraphs
        paragraphs = councillor_html.find_all("p")
        if len(paragraphs) < 2:
            raise SkipCouncillorException("Not enough data")

        division = paragraphs[0].get_text(strip=True)
        party = paragraphs[1].get_text(strip=True)

        # Create councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        # Try to extract email
        with contextlib.suppress(AttributeError, IndexError):
            email_link = soup.select_one("a[href^=mailto]")
            if email_link:
                councillor.email = email_link["href"].split(":")[-1]

        # Try to extract photo - look for the profile image
        with contextlib.suppress(AttributeError, TypeError):
            # Find image with alt text containing "Profile image"
            photo = soup.find("img", alt=lambda x: x and "Profile image" in x)
            if photo and photo.get("src"):
                img_src = photo["src"]
                if not img_src.startswith("data:"):
                    councillor.photo_url = urljoin(url, img_src)

        return councillor
