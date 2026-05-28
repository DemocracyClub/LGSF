import contextlib
from urllib.parse import urljoin

from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    verify_requests = False
    http_lib = "requests"

    def get_party_name(self, list_page_html):
        for img in reversed(list_page_html.find_all("img")):
            title = img.get("title") or img.get("alt", "")
            if title and "(logo)" in title:
                return title.replace("(logo)", "").strip()
        return "Unknown"

    def get_single_councillor(self, list_page_html):
        councillor = super().get_single_councillor(list_page_html)
        with contextlib.suppress(AttributeError, TypeError):
            for img in list_page_html.find_all("img"):
                title = img.get("title", "")
                alt = img.get("alt", "")
                if "(logo)" not in title and "(logo)" not in alt:
                    src = img.get("src")
                    if src:
                        councillor.photo_url = urljoin(self.base_url, src)
                        break
        return councillor
