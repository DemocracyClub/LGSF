from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "http://committeeadmin.lancaster.gov.uk"
    verify_requests = False
