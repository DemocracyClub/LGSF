from lgsf.minutes.scrapers import ModGovMinutesScraper
from lgsf.shared_sites import only_other_councils


class Scraper(ModGovMinutesScraper):
    """
    Worthing shares a ModernGov install with Adur, so leave out the committees
    whose names make them only Adur's: "Adur Planning Committee", "Joint
    Strategic Sub-Committee (Adur)". Joint committees are kept.
    """

    def exclude_committee_hook(self, record):
        return only_other_councils(record["title"], "Worthing", ["Adur"])
