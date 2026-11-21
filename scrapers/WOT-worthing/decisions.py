from lgsf.decisions.scrapers import ModGovDecisionsScraper
from lgsf.shared_sites import only_other_councils


class Scraper(ModGovDecisionsScraper):
    """
    Worthing shares a ModernGov install with Adur, so leave out the decisions
    whose decision maker is only Adur's ("Adur Leader", "Adur
    Planning Committee"). Decisions by both councils' cabinet members, by
    joint committees and by officers are kept.
    """

    def exclude_decision_hook(self, decision):
        return only_other_councils(decision.decision_maker, "Worthing", ["Adur"])
