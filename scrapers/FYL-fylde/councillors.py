import re

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://fylde.cmis.uk.com/fylde/CouncillorsandMP.aspx"

    def get_party_name(self, list_page_html):
        url = list_page_html.a["href"]
        page = self.get(url).text
        soup = BeautifulSoup(page, "html5lib")
        return (
            soup.find(text=re.compile("Political Party"))
            .parent.get_text(strip=True)
            .replace("Political Party:", "")
            .strip()
        )
