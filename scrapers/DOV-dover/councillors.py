from lgsf.scrapers.councillors import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    disabled = True
    base_url = "http://moderngov.dover.gov.uk"
