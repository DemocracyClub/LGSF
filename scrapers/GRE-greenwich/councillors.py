from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "http://committees.royalgreenwich.gov.uk"
    disabled = True
