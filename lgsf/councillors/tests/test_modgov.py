"""
Reading a councillor from ModernGov's GetCouncillorsByWard XML.
"""

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import ModGovCouncillorScraper


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
