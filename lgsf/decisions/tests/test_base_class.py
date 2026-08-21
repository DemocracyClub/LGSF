import pytest

from lgsf.decisions.scrapers import BaseDecisionsScraper, CustomHTMLDecisionsScraper


def test_abc_raises():
    with pytest.raises(TypeError) as excinfo:
        BaseDecisionsScraper(options={})
    assert "Can't instantiate abstract class BaseDecisionsScraper" in str(excinfo.value)


def test_custom_html_scraper_is_still_abstract():
    """It supplies page fetching, not the two methods a council must write."""
    with pytest.raises(TypeError) as excinfo:
        CustomHTMLDecisionsScraper(options={})
    assert "Can't instantiate abstract class CustomHTMLDecisionsScraper" in str(
        excinfo.value
    )


def test_decisions_accumulate():
    """
    A decision that drops out of the scrape window is still a fact, so runs
    must add to what is stored rather than replacing it.
    """
    from lgsf.storage.backends.base import StorageMode

    assert BaseDecisionsScraper.storage_mode == StorageMode.ACCUMULATE


class FakeConsole:
    def log(self, *args, **kwargs):
        pass


class RunnableScraper(BaseDecisionsScraper):
    """A scraper whose second decision page always fails."""

    def get_decisions(self):
        return [{"url": "a"}, {"url": "boom"}, {"url": "c"}]

    def get_single_decision(self, decision_data):
        if decision_data["url"] == "boom":
            raise ConnectionError("connection reset")
        return decision_data["url"]


def test_one_unreadable_page_does_not_lose_the_whole_run():
    """
    A council can have hundreds of decisions, each its own page fetch. If a
    single failure propagated, it would abandon the run before storage was
    finalised and lose every decision scraped so far.
    """
    scraper = RunnableScraper.__new__(RunnableScraper)
    scraper.console = FakeConsole()
    scraper.unchanged_decisions = 0
    scraper.failed_decisions = 0
    scraper.decisions = set()
    processed = []
    scraper.process_decision = lambda decision, raw: processed.append(decision)
    finalized = []
    scraper.save_index = lambda: None
    scraper.finalize_storage = finalized.append
    scraper.report = lambda: None

    scraper.run(run_log=object())

    assert processed == ["a", "c"]
    assert scraper.failed_decisions == 1
    assert len(finalized) == 1, "storage must still be finalised"
