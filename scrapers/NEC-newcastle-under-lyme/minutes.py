from lgsf.minutes.scrapers import ModGovMinutesScraper


class Scraper(ModGovMinutesScraper):
    # The site drops connections from AWS, from every region tried (Oct 2026).
    run_on_aws = False
