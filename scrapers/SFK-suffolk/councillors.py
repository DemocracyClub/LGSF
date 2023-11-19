from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.suffolk.gov.uk/council-and-democracy/councillors-and-elected-representatives/find-your-councillor/?ward=&action=SEARCH&party=&name="

    list_page = {
        "container_css_selector": ".results",
        "councillor_css_selector": ".councillor-row",
    }

    def get_single_councillor(self, councillor_html):
        url = "https://www.suffolk.gov.uk{}".format(
            councillor_html.find_all("a")[1]["href"]
        )

        identifier = url.rstrip("/").split("/")[-1]
        name = councillor_html.find_all("a")[0].text.strip()
        division = (
            councillor_html.find_all("p")[0].text.strip().split(":")[1].strip()
        )
        party = (
            councillor_html.find_all("p")[1].text.strip().split(":")[1].strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )
        req = self.get(url)
        soup = BeautifulSoup(req.text, "lxml")
        councillor.email = (
            soup.find(text="Email").findNext("p").getText(strip=True)
        )
        councillor.photo_url = soup.find("img", {"class": "img-responsive"})[
            "src"
        ]
        return councillor
