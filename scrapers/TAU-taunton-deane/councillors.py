from lgsf.scrapers.councillors import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    disabled = True
    base_url = "https://democracy.tauntondeane.gov.uk"

