from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    timeout = 60
    base_url = "https://democracy.north-herts.gov.uk"
