from bs4 import BeautifulSoup

from lgsf.scrapers.councillors import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.warrington.gov.uk/councillors/name"
    list_page = {
        "container_css_selector": "ul.item-list",
        "councillor_css_selector": "li",
    }

    def get_single_councillor(self, councillor_html):
        url = councillor_html.findAll("a")[0]["href"]
        req = self.get(url)
        soup = BeautifulSoup(req.text, "lxml")
        name = soup.h1.get_text(strip=True)
        division = soup.find(text="Ward:").next.strip()
        party = soup.find(text="Party:").next.strip()

        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        councillor.email = (
            soup.find(text="Email:").findNext("a").get_text(strip=True)
        )
        councillor.photo_url = "https:" + soup.img['src']
        return councillor
