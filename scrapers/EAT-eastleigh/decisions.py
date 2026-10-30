from lgsf.decisions.scrapers import ModGovDecisionsScraper


class Scraper(ModGovDecisionsScraper):
    # The site answers AWS with a 403, from every region tried (Oct 2026).
    run_on_aws = False
