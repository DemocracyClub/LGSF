from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    http_lib = "playwright"
    verify_requests = False
