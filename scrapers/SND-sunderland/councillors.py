from urllib.parse import urljoin

from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    verify_requests = False

    def get_party_name(self, list_page_html):
        return list_page_html.find_all("img")[-1]["alt"].replace("(logo)", "").strip()

    def get_single_councillor(self, list_page_html):
        councillor = super().get_single_councillor(list_page_html)
        img = list_page_html.find("img", {"class": "PenPicResize"})
        if img:
            src = img.get("data-src") or img.get("src")
            if src and not src.startswith("data:"):
                councillor.photo_url = urljoin(self.base_url, src)
        return councillor
