from lgsf.minutes.scrapers import CMISMinutesScraper


class Scraper(CMISMinutesScraper):
    verify_requests = False
