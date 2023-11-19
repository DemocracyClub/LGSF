import re
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.mansfield.gov.uk/councillors"
    list_page = {
        "container_css_selector": ".list--listing",
        "councillor_css_selector": ".list__item",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = councillor_html.h2.get_text(strip=True)

        if "Elected Mayor" in name:
            raise SkipCouncillorException()

        division = (
            councillor_html.find("strong", text=re.compile("Ward:"))
            .find_parent("p")
            .get_text(strip=True)
            .replace("Ward:", "")
            .strip()
        )

        party = (
            councillor_html.find("strong", text=re.compile("Party:"))
            .find_parent("p")
            .get_text(strip=True)
            .replace("Ward:", "")
            .strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )

        councillor.email = (
            soup.find("strong", text=re.compile("Email:"))
            .find_parent("p")
            .get_text(strip=True)
            .replace("Email:", "")
            .strip()
        )

        image_url = soup.select_one(".image--feature")["src"]

        councillor.photo_url = urljoin(self.base_url, image_url)

        return councillor
