import contextlib

from bs4 import BeautifulSoup
from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    http_lib = "requests"

    def get_single_councillor(self, list_page_html):
        url = list_page_html.a["href"]
        identifier = url.split("/id/")[1].split("/")[0]
        name = list_page_html.find("div", {"class": "NameLink"}).getText(strip=True)
        if "Vacancy" in name:
            raise SkipCouncillorException("Vacancy")
        division = list_page_html.find(text=self.division_text).next.strip()
        party = self.get_party_name(list_page_html)

        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )

        text = self.get_text(url)
        soup = BeautifulSoup(text, "lxml")
        with contextlib.suppress(IndexError):
            councillor.email = soup.select(".Email")[0].getText(strip=True)
        img = soup.find("img", src=lambda s: s and "ViewCMIS_PersonPenPic" in s)
        if img:
            councillor.photo_url = img["src"]
        return councillor
