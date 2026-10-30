from lgsf.minutes.scrapers import ModGovMinutesScraper


class Scraper(ModGovMinutesScraper):
    # The site answers AWS with a 403, from every region tried (Oct 2026).
    run_on_aws = False
