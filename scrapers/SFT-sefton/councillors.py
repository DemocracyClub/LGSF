from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    verify_requests = False
    base_url = "https://modgov.sefton.gov.uk"
