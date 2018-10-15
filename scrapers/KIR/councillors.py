from lgsf.scrapers.councillors import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "http://democracy.kirklees.gov.uk"

    tags = ["example"]
