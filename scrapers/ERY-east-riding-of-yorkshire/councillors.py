from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.eastriding.gov.uk/council/councillors-and-members-of-parliament/find-a-councillor/"

    list_page = {
        "container_css_selector": ".er-filter-grid",
        "councillor_css_selector": ".er-filter-row",
    }

    def get_single_councillor(self, councillor_html):
        url = councillor_html.select_one("a")
        if not url:
            raise SkipCouncillorException("Header row")
        url = urljoin(self.base_url, url["href"].strip())
        soup = self.get_page(url).select_one("#entry")
        name = soup.h1.get_text(strip=True).replace("Councillor ", "")
        name = " ".join(name.split(" ")[1::-1]).replace(",", "")

        division = soup.find("span", text="Ward").find_next("div").get_text(strip=True)
        party = (
            soup.find("span", text="Political Party")
            .find_next("div")
            .get_text(strip=True)
        )
        councillor = self.add_councillor(
            url, identifier=url, party=party, division=division, name=name
        )
        councillor.email = soup.select("a[href^=mailto]")[0].get_text(strip=True)
        councillor.photo_url = soup.select_one("img")["src"]
        return councillor
