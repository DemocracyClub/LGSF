import pytest

from lgsf.minutes.scrapers import BaseMinutesScraper, CustomHTMLMinutesScraper


def test_abc_raises():
    with pytest.raises(TypeError) as excinfo:
        BaseMinutesScraper(options={})
    assert "Can't instantiate abstract class BaseMinutesScraper" in str(excinfo.value)


def test_custom_html_scraper_is_still_abstract():
    """It supplies page fetching, not the two methods a council must write."""
    with pytest.raises(TypeError) as excinfo:
        CustomHTMLMinutesScraper(options={})
    assert "Can't instantiate abstract class CustomHTMLMinutesScraper" in str(
        excinfo.value
    )


def test_custom_html_scraper_is_tagged_html():
    """--tags html selects these, as it does for councillors."""
    assert CustomHTMLMinutesScraper.class_tags == ["html"]
