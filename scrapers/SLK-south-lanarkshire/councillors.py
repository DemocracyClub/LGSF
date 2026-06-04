from urllib.parse import urljoin

from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    verify_requests = False

    def get_single_councillor(self, list_page_html):
        councillor = super().get_single_councillor(list_page_html)
        photo_img = list_page_html.find("img", class_="PenPicResize")
        if photo_img and photo_img.get("src"):
            councillor.photo_url = urljoin(self.base_url, photo_img["src"])
        return councillor
