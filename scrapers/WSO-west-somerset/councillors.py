from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    disabled = True
    base_url = "https://democracy.westsomerset.gov.uk"
