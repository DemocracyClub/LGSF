from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    disabled = True
    base_url = "https://ardsandnorthdown.gov.uk/article/1631/Councillor-Search"
    list_page = {
        "container_css_selector": ".grid--searchgrid",
        "councillor_css_selector": ".grid__cell--searchitem-article",
    }

    def get_single_councillor(self, councillor_html):
        councillor_url = councillor_html.select_one("a")["href"]
        url = urljoin(self.base_url, councillor_url)
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True)

        party, division = (
            soup.select_one(".councillor-party")
            .get_text(strip=True)
            .split(" | ")
        )
        councillor = self.add_councillor(
            url=url, identifier=url, name=name, division=division, party=party
        )

        container = soup.select_one(".councillor-specific")

        councillor.email = container.select_one("a[href^=mailto]")[
            "href"
        ].replace("mailto:", "")

        councillor.photo_url = container.select_one("img")["src"]

        return councillor
