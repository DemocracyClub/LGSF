from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    # The site drops connections from AWS, from every region tried (Oct 2026).
    run_on_aws = False
