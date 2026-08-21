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
