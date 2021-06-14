from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "http://democracy.towerhamlets.gov.uk"
    disabled = False
