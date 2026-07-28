from urllib.parse import urljoin

from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    http_lib = "playwright"
    division_text = "Division:"

    def get_single_councillor(self, list_page_html):
        councillor = super().get_single_councillor(list_page_html)
        img = list_page_html.find("img", alt=lambda x: x and "(PenPic)" in x)
        if img:
            src = img.get("data-src") or img.get("src")
            if src and not src.startswith("data:"):
                councillor.photo_url = urljoin(self.base_url, src)
        return councillor
