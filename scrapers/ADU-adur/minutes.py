from lgsf.minutes.scrapers import ModGovMinutesScraper
from lgsf.shared_sites import only_other_councils


class Scraper(ModGovMinutesScraper):
    """
    Adur shares a ModernGov install with Worthing, so leave out the committees
    whose names make them only Worthing's: "Worthing Planning Committee", "Joint
    Strategic Sub-Committee (Worthing)". Joint committees are kept.
    """

    def exclude_committee_hook(self, record):
        return only_other_councils(record["title"], "Adur", ["Worthing"])
