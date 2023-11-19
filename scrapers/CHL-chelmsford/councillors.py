import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.chelmsford.gov.uk/your-council/councillors-committees-and-decision-making/councillors/find-a-councillor/"

    def get_councillors(self):
        url = self.base_url
        soup = self.get_page(url)
        drc = soup.find("input", {"name": "drc"})["value"]
        tgt = soup.find("input", {"name": "tgt"})["value"]
        next_page = 1
        while next_page:
            results = self.requests_session.post(
                "https://www.chelmsford.gov.uk/api/directories/search",
                data={"drc": drc, "tgt": tgt, "page": next_page},
            )
            page_soup = BeautifulSoup(results.text, "html5lib")
            if page_soup.find(text="No records found"):
                break
            next_page += 1
            for row in page_soup.select("tbody tr"):
                yield row

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True).replace("Councillor ", "")
        if name == "Vacant councillor post":
            raise SkipCouncillorException("Vacant")
        division = (
            soup.find("h2", text=re.compile("Ward"))
            .find_next("p")
            .get_text(strip=True)
        )
        party = (
            soup.find("h2", text=re.compile("Political party"))
            .find_next("p")
            .get_text(strip=True)
        )
        councillor = self.add_councillor(
            url, identifier=url, name=name, division=division, party=party
        )
        email = soup.find("h2", text=re.compile("Email address"))
        if email:
            councillor.email = email.find_next("a")["href"]
        councillor.photo_url = urljoin(
            self.base_url, soup.select_one("picture img")["src"]
        )
        return councillor
