from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    verify_requests = False
    base_url = "https://democraticservices.hounslow.gov.uk"
