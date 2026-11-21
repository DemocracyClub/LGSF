from lgsf.decisions.scrapers import ModGovDecisionsScraper
from lgsf.shared_sites import only_other_councils


class Scraper(ModGovDecisionsScraper):
    """
    Adur shares a ModernGov install with Worthing, so leave out the decisions
    whose decision maker is only Worthing's ("Worthing Leader", "Worthing
    Planning Committee"). Decisions by both councils' cabinet members, by
    joint committees and by officers are kept.
    """

    def exclude_decision_hook(self, decision):
        return only_other_councils(decision.decision_maker, "Adur", ["Worthing"])
