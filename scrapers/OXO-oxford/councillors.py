from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://mycouncil.oxford.gov.uk"
    verify_requests = False
