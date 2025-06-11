from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://lbbd.moderngov.co.uk/"
    verify_requests = False
