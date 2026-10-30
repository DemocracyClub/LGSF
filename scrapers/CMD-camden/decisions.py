from lgsf.decisions.scrapers import ModGovDecisionsScraper


class Scraper(ModGovDecisionsScraper):
    # The site answers AWS with a 403 on every run (Oct 2026).
    run_on_aws = False
