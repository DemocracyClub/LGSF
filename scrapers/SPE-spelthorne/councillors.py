from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://democracy.spelthorne.gov.uk"

    # Server timeout 2024-04-27
    disabled = True
