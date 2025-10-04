from lgsf.councillors.scrapers import ModGovCouncillorScraper

class Scraper(ModGovCouncillorScraper):
    def get(self, url, extra_headers=None):
        from curl_cffi import requests

        return requests.get(url, impersonate="chrome")
