from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://modgov.waverley.gov.uk"
    verify_requests = False
