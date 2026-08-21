class SkipDecisionException(Exception):
    """Raised by a scraper to drop a decision from the run entirely."""


class DecisionNotModifiedException(Exception):
    """
    Raised when the server answered 304 to a conditional request for a
    decision, meaning what we already have stored is still current and there
    is nothing to re-scrape.
    """
