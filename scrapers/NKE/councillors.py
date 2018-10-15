from lgsf.scrapers.councillors import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    disabled = False

    base_url = "https://nkdc.moderngov.co.uk/"
