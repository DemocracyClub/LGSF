from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    verify_requests = False

    def get_party_name(self, list_page_html):
        for img in reversed(list_page_html.find_all("img")):
            title = img.get("title") or img.get("alt", "")
            if title and "(logo)" in title:
                return title.replace("(logo)", "").strip()
        return "Unknown"
