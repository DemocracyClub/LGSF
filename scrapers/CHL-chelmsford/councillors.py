import re
from urllib.parse import urljoin


from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import PagedHTMLCouncillorScraper


class Scraper(PagedHTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "table.directories-table__table",
        "councillor_css_selector": "tr",
        "next_page_css_selector": "ul.paging li:last-child",
    }

    def get_single_councillor(self, councillor_html):
        if councillor_html.find_all("th"):
            raise SkipCouncillorException
        print(councillor_html)
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True).replace("Councillor ", "")
        if name == "Vacant councillor post":
            raise SkipCouncillorException("Vacant")
        division = (
            soup.find("h2", text=re.compile("Ward")).find_next("p").get_text(strip=True)
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
