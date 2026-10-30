from lgsf.decisions.scrapers import ModGovDecisionsScraper


class Scraper(ModGovDecisionsScraper):
    # The site drops connections from AWS, from every region tried (Oct 2026).
    run_on_aws = False
