from lgsf.minutes.scrapers import ModGovMinutesScraper


class Scraper(ModGovMinutesScraper):
    # The site rate-limits with 429s partway through a run; retrying
    # honours its Retry-After.
    retries = 4
