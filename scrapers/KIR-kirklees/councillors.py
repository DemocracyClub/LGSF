from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "http://democracy.kirklees.gov.uk"

    tags = ["example"]
