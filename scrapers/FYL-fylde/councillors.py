import re

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    def get_party_name(self, list_page_html):
        url = list_page_html.a["href"]
        text = self.get_text(url)
        soup = BeautifulSoup(text, "html5lib")

        # Try to find "Political Party:" in the profile text
        political_party_text = soup.find(text=re.compile("Political Party"))
        if political_party_text:
            return (
                political_party_text.parent.get_text(strip=True)
                .replace("Political Party:", "")
                .strip()
            )

        # Fallback: get party from the "Value Party" div
        party_div = soup.find("div", {"class": "Value Party"})
        if party_div:
            return party_div.get_text(strip=True)

        # If neither found, return empty string
        return ""
