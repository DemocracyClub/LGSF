from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://democracy.basingstoke.gov.uk"
    verify_requests = False
