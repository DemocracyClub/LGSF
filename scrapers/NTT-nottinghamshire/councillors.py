import contextlib
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    verify_requests = False
    division_text = "Division:"

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

        soup = BeautifulSoup(self.get_text(url), "lxml")
        with contextlib.suppress(IndexError):
            councillor.email = soup.select(".Email")[0].getText(strip=True)

        photo = soup.select_one("img.PenPicResize")
        if photo and photo.get("src"):
            councillor.photo_url = urljoin(url, photo["src"])

        return councillor
