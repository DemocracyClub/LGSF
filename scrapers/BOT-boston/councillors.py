from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://democracy.boston.gov.uk"
    verify_requests = False
