"""
Matching our councils to Poteris's and choosing each one's start date.
No network: Poteris's councils are given as the API returns them.
"""

import datetime

from scripts.poteris_since import Council, match, normalise, start_date

TODAY = datetime.date(2026, 10, 23)


def poteris(id, name, url):
    return {"id": id, "name": name, "base_scrape_url": url}


def test_names_normalise_across_the_ways_councils_are_written():
    assert normalise("Adur District Council") == normalise("Adur") == "adur"
    assert normalise("London Borough of Newham") == "newham"
    assert normalise("Brighton & Hove City Council") == normalise("Brighton and Hove")


def test_a_council_matches_on_its_host_and_name():
    ours = [
        Council("ADU", "Adur", {"democracy.adur-worthing.gov.uk"}),
        Council("WOT", "Worthing", {"democracy.adur-worthing.gov.uk"}),
    ]
    theirs = [
        poteris(
            123,
            "Adur",
            "https://democracy.adur-worthing.gov.uk/mgCalendarMonthView.aspx",
        ),
        poteris(
            124,
            "Worthing",
            "https://democracy.adur-worthing.gov.uk/mgCalendarMonthView.aspx",
        ),
    ]

    matched = match(ours, theirs)

    assert {code: (c["id"], how) for code, (c, how) in matched.items()} == {
        "ADU": (123, "host+name"),
        "WOT": (124, "host+name"),
    }


def test_a_host_alone_is_not_enough():
    """How Bournemouth, Christchurch and Poole once matched Fife."""
    ours = [
        Council("BPC", "Bournemouth, Christchurch and Poole", {"shared.host.example"})
    ]
    theirs = [poteris(1, "Fife", "https://shared.host.example/x")]

    assert match(ours, theirs) == {}


def test_a_council_on_a_different_host_matches_by_name_alone():
    ours = [Council("KIR", "Kirklees", {"democracy.kirklees.gov.uk"})]
    theirs = [poteris(7, "Kirklees", "https://kirklees.moderngov.co.uk/x")]

    assert match(ours, theirs)["KIR"][1] == "name"


def test_the_start_is_a_year_back_when_poteris_is_more_recent():
    assert start_date(datetime.date(2026, 9, 30), TODAY, 90, 1) == datetime.date(
        2025, 10, 23
    )


def test_the_start_is_poteris_newest_less_the_margin_when_older_than_a_year():
    assert start_date(datetime.date(2024, 3, 28), TODAY, 90, 1) == datetime.date(
        2023, 12, 29
    )


def test_a_council_poteris_doesnt_have_starts_a_year_back():
    assert start_date(None, TODAY, 90, 1) == datetime.date(2025, 10, 23)
