"""
Not re-fetching decisions that have settled.

A decision is amended, if at all, in the months just after publication, so
one older than that which we already hold is left alone. Everything here is
about not skipping something we would still want.
"""

import datetime
from pathlib import Path

import pytest

from lgsf.decisions.scrapers import ModGovDecisionsScraper

URL = "https://example.gov.uk/ieDecisionDetails.aspx?ID=42"


def days_ago(days):
    return (datetime.date.today() - datetime.timedelta(days=days)).isoformat()


class FakeSession:
    """A metadata store holding a known set of filenames."""

    def __init__(self, present=()):
        self.present = set(present)
        self.opened = []

    def open(self, filename, mode="r"):
        self.opened.append(str(filename))
        if Path(filename).name not in self.present:
            raise FileNotFoundError(filename)
        return "{}"


def make_scraper(index_entry=None, stored=(), settled_after_months=3):
    scraper = ModGovDecisionsScraper.__new__(ModGovDecisionsScraper)
    scraper.settled_after_months = settled_after_months
    scraper.storage_session = FakeSession(stored)
    scraper.index = {"decisions": {}, "documents": {}}
    if index_entry is not None:
        scraper.index["decisions"][URL] = index_entry
    return scraper


def row(published):
    return {"url": URL, "identifier": "42", "published_date": published}


def test_an_old_decision_we_hold_is_settled():
    scraper = make_scraper(
        index_entry={"file_name": "2020-01-01-42.json"},
        stored=["2020-01-01-42.json"],
    )

    assert scraper.is_settled(row(days_ago(400))) is True


def test_a_recent_decision_is_refetched_even_though_we_hold_it():
    """Inside the window a status can still move or a report be attached."""
    scraper = make_scraper(
        index_entry={"file_name": "2020-01-01-42.json"},
        stored=["2020-01-01-42.json"],
    )

    assert scraper.is_settled(row(days_ago(30))) is False


def test_the_boundary_is_three_months():
    scraper = make_scraper(index_entry={"file_name": "x.json"}, stored=["x.json"])

    just_inside = (datetime.date.today() - datetime.timedelta(days=89)).isoformat()
    well_outside = (datetime.date.today() - datetime.timedelta(days=100)).isoformat()

    assert scraper.is_settled(row(just_inside)) is False
    assert scraper.is_settled(row(well_outside)) is True


def test_an_old_decision_we_do_not_hold_is_fetched():
    """Nothing in the index: this is the first time we have seen it."""
    scraper = make_scraper()

    assert scraper.is_settled(row(days_ago(400))) is False


def test_a_decision_that_previously_failed_is_retried_however_old():
    """
    A failed run records validators but never a file_name, so the decision
    is never mistaken for one we hold.
    """
    scraper = make_scraper(index_entry={"etag": '"abc"', "last_modified": None})

    assert scraper.is_settled(row(days_ago(400))) is False


def test_a_record_deleted_from_storage_is_fetched_again():
    """The index is a cache. Its word is not taken without checking."""
    scraper = make_scraper(index_entry={"file_name": "2020-01-01-42.json"}, stored=[])

    assert scraper.is_settled(row(days_ago(400))) is False


def test_a_row_with_no_published_date_is_fetched():
    scraper = make_scraper(index_entry={"file_name": "x.json"}, stored=["x.json"])

    assert scraper.is_settled(row(None)) is False


def test_settling_can_be_turned_off():
    """settled_after_months = None re-fetches everything in the window."""
    scraper = make_scraper(
        index_entry={"file_name": "x.json"},
        stored=["x.json"],
        settled_after_months=None,
    )

    assert scraper.settled_before is None
    assert scraper.is_settled(row(days_ago(400))) is False


def test_the_check_costs_no_http_request():
    """
    Publication date comes from the list page, so a settled decision is
    identified without fetching its page. If it needed one, the saving
    would be nothing.
    """
    scraper = make_scraper(index_entry={"file_name": "x.json"}, stored=["x.json"])

    def fail(*args, **kwargs):
        raise AssertionError("should not make a request")

    scraper.get_text = fail
    scraper.get_conditional = fail

    assert scraper.is_settled(row(days_ago(400))) is True


@pytest.mark.parametrize("months,days,expected", [(1, 40, True), (12, 40, False)])
def test_the_settling_period_is_configurable(months, days, expected):
    scraper = make_scraper(
        index_entry={"file_name": "x.json"},
        stored=["x.json"],
        settled_after_months=months,
    )

    assert scraper.is_settled(row(days_ago(days))) is expected
