from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    disabled = False
    list_page = {
        "container_css_selector": ".container .col-md-8",
        "councillor_css_selector": ".col-sm-4",
    }

    def get_single_councillor(self, councillor_html):
        url = "https://www.angus.gov.uk{}".format(
            councillor_html.find_all("a")[0]["href"]
        )
        identifier = url.split("/")[-1]
        name = councillor_html.find_all("a")[0].text
        division = councillor_html.find("div", {"class": "councillor-ward"}).text
        party = councillor_html.find("div", {"class": "councillor-party"}).text
        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )
        req = self.get(url)
        soup = BeautifulSoup(req.text, "lxml")
        councillor.email = soup.select(".field--name-field-email a[href^=mailto]")[
            0
        ].get_text(strip=True)

        return councillor
