from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://modgov.lbbd.gov.uk/internet"
    verify_requests = False
    disabled = True
