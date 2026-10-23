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


def test_since_starts_the_date_range_for_a_backfill():
    import datetime

    from lgsf.minutes.scrapers import BaseMinutesScraper

    class Minutes(BaseMinutesScraper):
        def get_meetings(self):
            return []

        def get_single_meeting(self, row):
            return None

    scraper = Minutes.__new__(Minutes)
    today = datetime.date.today()

    scraper.options = {}
    start, end = scraper.date_range
    assert start == today - datetime.timedelta(weeks=BaseMinutesScraper.weeks_back)

    scraper.options = {"since": "2019-05-01"}
    start, end = scraper.date_range
    assert start == datetime.date(2019, 5, 1)
    assert end == today + datetime.timedelta(weeks=BaseMinutesScraper.weeks_forward)


def test_the_command_takes_since():
    import io

    from lgsf.minutes.commands import Command

    command = Command(["", "--council", "KIR", "--since", "2019-05-01"], io.StringIO())
    assert vars(command.create_parser())["since"] == "2019-05-01"
