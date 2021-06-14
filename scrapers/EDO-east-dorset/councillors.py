from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    disabled = True
    base_url = "http://moderngoveddc.christchurchandeastdorset.gov.uk"
