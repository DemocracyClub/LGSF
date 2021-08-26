from urllib.parse import urljoin

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.argyll-bute.gov.uk/councillor_list"
    list_page = {
        "container_css_selector": ".view-councillors-list",
        "councillor_css_selector": "tbody tr",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.findAll("a")[0]["href"])
        req = self.get(url)
        soup = BeautifulSoup(req.text, "lxml")
        # print(soup)
        name = soup.select("h1#page-title")[0].get_text(strip=True)
        division = " ".join(
            councillor_html.select("td.views-field-field-ward")[0]
            .get_text(strip=True)
            .split(" ")[1:]
        )
        party = councillor_html.select("td strong")[1].get_text()

        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        councillor.email = soup.select("a[href^=mailto]")[0].get_text(strip=True)
        councillor.photo_url = soup.select(".node-councillors")[0].img["src"]
        return councillor
