import re

from bs4 import BeautifulSoup

from lgsf.scrapers.councillors import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    disabled = False
    base_url = "https://www.stroud.gov.uk/council-and-democracy/your-councillors/your-councillors-by-name"  # noqa
    list_page = {
        "container_css_selector": ".container .col-md-8",
        "councillor_css_selector": ".col-sm-3",
    }

    def get_single_councillor(self, councillor_html):
        url = "https://www.stroud.gov.uk{}".format(
            councillor_html.find_all("a")[1]["href"]
        )
        identifier = url.split("/")[-1]
        name = councillor_html.find_all("a")[1].text
        division = (
            re.search("Ward: (.*)", councillor_html.find("p").text)
            .group(1)
            .strip()
        )
        party = (
            re.search("Political Party: (.*)", councillor_html.find("p").text)
            .group(1)
            .strip()
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
            soup.find(text="Email:").findNext("td").getText(strip=True)
        )

        return councillor
