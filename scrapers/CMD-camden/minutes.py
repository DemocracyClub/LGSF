from lgsf.minutes.scrapers import ModGovMinutesScraper


class Scraper(ModGovMinutesScraper):
    # The site answers AWS with a 403 on every run (Oct 2026).
    run_on_aws = False
