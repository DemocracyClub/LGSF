from lgsf.scrapers.councillors import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "http://democracy.towerhamlets.gov.uk"
    disabled = False
