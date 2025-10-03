from urllib.parse import urljoin

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.suffolk.gov.uk/council-and-democracy/councillors-and-elected-representatives/find-your-councillor/?ward=&action=SEARCH&party=&name="

    list_page = {
        "container_css_selector": ".results",
        "councillor_css_selector": ".result",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.select_one("a")["href"])

        identifier = url.rstrip("/").split("/")[-1]
        name = councillor_html.find_all("a")[0].text.strip()
        division = councillor_html.select_one(".division .value").get_text(strip=True)
        party = councillor_html.select_one(".party .value").get_text(strip=True)

        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )
        req = self.get(url)
        soup = BeautifulSoup(req.text, "lxml")
        councillor.email = soup.select_one("a[href^=mailto]")["href"].replace(
            "mailto:", ""
        )

        councillor.photo_url = soup.select_one(".councillor__profile img")["src"]
        return councillor
