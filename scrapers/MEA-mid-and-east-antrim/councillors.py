from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.midandeastantrim.gov.uk/council/councillors"
    list_page = {
        "container_css_selector": ".list-councillors",
        "councillor_css_selector": ".col-4",
    }

    def get_single_councillor(self, councillor_html):
        councillor_url = councillor_html.select_one("a")["href"]
        url = urljoin(self.base_url, councillor_url)
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True)

        party, division = (
            soup.select_one(".councillor-party").get_text(strip=True).split(" | ")
        )
        councillor = self.add_councillor(
            url=url, identifier=url, name=name, division=division, party=party
        )

        container = soup.select_one(".councillor-specific")

        councillor.email = container.select_one("a[href^=mailto]")["href"].replace(
            "mailto:", ""
        )

        councillor.photo_url = container.select_one("img")["src"]

        return councillor
