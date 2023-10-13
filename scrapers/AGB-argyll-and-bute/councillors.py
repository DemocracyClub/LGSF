from urllib.parse import urljoin

from bs4 import BeautifulSoup

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper, PagedHTMLCouncillorScraper


class Scraper(PagedHTMLCouncillorScraper):
    base_url = "https://www.argyll-bute.gov.uk/councillor_list"
    list_page = {
        "container_css_selector": ".localgov-directory",
        "councillor_css_selector": ".views-row",
        "next_page_css_selector": ".pager__item--next",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.findAll("a")[0]["href"])
        req = self.get(url)
        soup = BeautifulSoup(req.text, "lxml")
        name = soup.select_one("h1.lgd-page-title-block__title").get_text(strip=True)
        if name == "Vacant":
            raise SkipCouncillorException("Vacant")

        division = " ".join(
            councillor_html.select_one(".field--name-field-ward")
            .get_text(strip=True)
        )
        party = councillor_html.select_one(".field--name-localgov-directory-facets-select").get_text()

        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        councillor.email = soup.select("a[href^=mailto]")[0].get_text(strip=True)
        councillor.photo_url = soup.select_one(".field--name-field-photo").img["src"]
        return councillor
