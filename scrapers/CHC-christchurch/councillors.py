from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "http://moderngovcbc.christchurchandeastdorset.gov.uk"
    disabled = True
