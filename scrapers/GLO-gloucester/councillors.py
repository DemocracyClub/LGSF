from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    # https://github.com/DemocracyClub/LGSF/issues/79
    disabled = True
    base_url = "http://democracy.gloucester.gov.uk"
