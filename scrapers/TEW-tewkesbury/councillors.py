from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "http://minutes.tewkesbury.gov.uk"
    disabled = True
