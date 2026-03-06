import contextlib

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    def get_single_councillor(self, list_page_html):
        councillor = super().get_single_councillor(list_page_html)
        url = list_page_html.a["href"]
        soup = BeautifulSoup(self.get_text(url), "lxml")
        with contextlib.suppress(StopIteration):
            councillor.email = next(
                link["href"].replace("mailto:", "")
                for link in soup.select("a[href^=mailto]")
                if "memberssecs" not in link["href"]
            )
        return councillor
