from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    def get_party_name(self, list_page_html):
        """
        Override to handle cases where party logo image is missing.
        Falls back to fetching from detail page if no logo found.
        """
        imgs = list_page_html.find_all("img")
        # Check if there's a party logo (last image with title attribute)
        if imgs and imgs[-1].get("title"):
            return imgs[-1]["title"].replace("(logo)", "").strip()

        # No party logo found, fetch from detail page
        url = list_page_html.a["href"]
        text = self.get_text(url, extra_headers=self.extra_headers)
        soup = BeautifulSoup(text, "lxml")
        party_label = soup.find(text="Party:")
        if party_label and party_label.parent:
            party_div = party_label.parent.find_next_sibling()
            if party_div:
                party_text = party_div.get_text(strip=True)
                if party_text:
                    return party_text

        return "Unknown"
