from lgsf.minutes.scrapers import ModGovMinutesScraper


class Scraper(ModGovMinutesScraper):
    # The server drops connections now and then (broken pipe, timeouts). A
    # run makes a request per committee, so one dropped request failed the
    # whole run in each of seven daily runs; retry it instead.
    retries = 2
