from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    timeout = 30
    use_proxy = True  # Council blocks AWS Lambda IPs
