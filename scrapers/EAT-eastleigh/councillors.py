from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    # The site answers AWS with a 403, from every region tried (Oct 2026).
    run_on_aws = False
