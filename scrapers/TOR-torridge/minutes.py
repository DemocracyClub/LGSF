from lgsf.minutes.scrapers import ModGovMinutesScraper
from lgsf.scrapers.base import ScraperBase


class Scraper(ModGovMinutesScraper):
    verify_requests = False
    # The site's firewall turns away the odd request with a 403 partway
    # through a run; the same request a few seconds later goes through.
    retries = 4
    RETRY_STATUSES = ScraperBase.RETRY_STATUSES | {403}
