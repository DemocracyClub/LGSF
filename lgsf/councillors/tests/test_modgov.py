"""
Reading a councillor from ModernGov's GetCouncillorsByWard XML.
"""

import datetime

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import ModGovCouncillorScraper

TODAY = datetime.date(2026, 11, 21)


def terms_xml(*terms):
    rows = "".join(
        f"<termofoffice><startdate>{start}</startdate>"
        f"<enddate>{end}</enddate></termofoffice>"
        for start, end in terms
    )
    return BeautifulSoup(
        f"<councillor><termsofoffice>{rows}</termsofoffice></councillor>",
        features="xml",
    ).find("councillor")


def term(*terms):
    scraper = ModGovCouncillorScraper.__new__(ModGovCouncillorScraper)
    start, end = scraper.term_of_office(terms_xml(*terms), today=TODAY)
    return (
        start.date().isoformat() if start else None,
        end.date().isoformat() if end else None,
    )


def ward_xml(startdate, enddate):
    return BeautifulSoup(
        f"""
        <ward>
          <wardtitle>Abbey</wardtitle>
          <councillor>
            <councillorid>123</councillorid>
            <fullusername>Councillor Jo Bloggs</fullusername>
            <politicalpartytitle>Independent</politicalpartytitle>
            <termsofoffice>
              <termofoffice>
                <startdate>{startdate}</startdate>
                <enddate>{enddate}</enddate>
              </termofoffice>
            </termsofoffice>
          </councillor>
        </ward>
        """,
        features="xml",
    ).find("ward")


def read(ward):
    scraper = ModGovCouncillorScraper.__new__(ModGovCouncillorScraper)
    scraper.base_url = "https://democracy.example.gov.uk"
    scraper.councillors = set()
    return scraper.get_single_councillor(ward, ward.find("councillor"))


def test_start_and_end_dates_are_read():
    councillor = read(ward_xml("05/05/2022", "04/05/2026"))
    assert councillor.start_date == "2022-05-05T00:00:00"
    assert councillor.standing_down == "2026-05-04T00:00:00"


def test_an_unspecified_start_date_is_left_unset():
    """Some installs record no start for a term, as they do for its end."""
    councillor = read(ward_xml("unspecified", "unspecified"))
    assert getattr(councillor, "start_date", None) is None
    assert getattr(councillor, "standing_down", None) is None


def test_a_later_term_with_no_end_is_current():
    """Reading's David Stevens: lost his seat in 2022, back in 2024."""
    assert term(("03/05/2018", "05/05/2022"), ("03/05/2024", "unspecified")) == (
        "2024-05-03",
        None,
    )


def test_the_current_term_is_found_wherever_it_is_listed():
    assert term(("03/05/2024", "unspecified"), ("03/05/2018", "05/05/2022")) == (
        "2024-05-03",
        None,
    )
    assert term(("07/05/2023", "11/05/2027"), ("02/05/2019", "06/05/2023")) == (
        "2023-05-07",
        "2027-05-11",
    )


def test_a_term_that_ends_before_it_starts_keeps_only_its_end():
    """
    Thanet's Rob Yates and Flintshire's Mared Eastwood (2107 for 2022):
    long over. West Oxfordshire's Duncan Enright: a term to 2028.
    """
    assert term(
        ("03/05/2019", "04/05/2023"),
        ("05/05/2023", "unspecified"),
        ("05/05/2023", "04/05/2023"),
    ) == ("2023-05-05", None)
    assert term(("09/05/2022", "unspecified"), ("09/05/2107", "09/05/2022")) == (
        "2022-05-09",
        None,
    )
    assert term(
        ("03/05/2012", "05/05/2021"),
        ("06/05/2021", "06/05/2024"),
        ("07/05/2028", "04/05/2028"),
    ) == (None, "2028-05-04")


def test_an_older_term_left_open_gives_way_to_the_latest():
    """Hertsmere's Caroline Clapper; Peterborough's blank terms."""
    assert term(
        ("03/05/2012", "07/05/2019"),
        ("07/05/2019", "unspecified"),
        ("09/05/2023", "10/05/2027"),
    ) == ("2023-05-09", "2027-05-10")
    assert term(
        ("unspecified", "unspecified"),
        ("02/05/2019", "04/05/2023"),
        ("05/05/2023", "01/05/2027"),
    ) == ("2023-05-05", "2027-05-01")


def test_with_no_current_term_the_last_one_to_end_is_given():
    """A former councillor: the website's word that they have left."""
    assert term(("02/05/2019", "06/05/2023"), ("03/05/2015", "07/05/2019")) == (
        "2019-05-02",
        "2023-05-06",
    )


def test_a_term_still_to_begin_is_given_before_one_that_ended():
    """Elected, not yet in office: the old term has just ended."""
    assert term(("02/05/2022", "20/11/2026"), ("24/11/2026", "unspecified")) == (
        "2026-11-24",
        None,
    )


def test_no_terms():
    assert term() == (None, None)
    assert term(("unspecified", "unspecified")) == (None, None)
