"""
Parsing a ModernGov decision, from the list page to the record.

Fixtures are real responses from democracy.kirklees.gov.uk, trimmed to the
relevant markup.
"""

import datetime
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from lgsf.decisions.exceptions import DecisionNotModifiedException
from lgsf.decisions.scrapers import ModGovDecisionsScraper

FIXTURES = Path(__file__).parent / "fixtures"
BASE_URL = "https://democracy.example.gov.uk"


def fixture(name):
    return (FIXTURES / name).read_text()


class FakeConsole:
    def log(self, *args, **kwargs):
        pass


class FakeResponse:
    def __init__(self, text, status_code=200, headers=None):
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}


def make_scraper(pages=None):
    """
    A scraper with the HTTP layer faked, driving the real parsing methods.

    ScraperBase.__init__ wants council metadata, a storage backend and an
    HTTP client, none of which a parsing test needs.
    """
    scraper = ModGovDecisionsScraper.__new__(ModGovDecisionsScraper)
    scraper.base_url = BASE_URL
    scraper.decisions = set()
    scraper.console = FakeConsole()
    scraper.extra_headers = {}
    scraper.options = {}
    pages = pages or {}

    scraper.get_text = lambda url, **kwargs: pages[url]
    return scraper


def test_list_page_yields_one_row_per_decision():
    scraper = make_scraper()
    soup = BeautifulSoup(fixture("modgov_decisions_list.html"), "lxml")

    rows = list(scraper.decision_rows(soup, f"{BASE_URL}/mgDelegatedDecisions.aspx"))

    assert [r["identifier"] for r in rows] == ["13567", "13149"]


def test_the_hidden_ref_span_is_not_part_of_the_title():
    """
    Each link ends with a hidden `ref: NNNN` span. Left in, every title
    reads "...Huddersfieldref: 13567".
    """
    scraper = make_scraper()
    soup = BeautifulSoup(fixture("modgov_decisions_list.html"), "lxml")

    rows = list(scraper.decision_rows(soup, f"{BASE_URL}/mgDelegatedDecisions.aspx"))

    assert rows[1]["title"] == "Admission of the Public"
    assert "ref:" not in rows[0]["title"]
    assert rows[0]["title"].endswith("Huddersfield")


def test_rows_that_are_not_decisions_are_ignored():
    """An issue-history link is not a decision and has no ID parameter."""
    scraper = make_scraper()
    soup = BeautifulSoup(fixture("modgov_decisions_list.html"), "lxml")

    rows = list(scraper.decision_rows(soup, f"{BASE_URL}/mgDelegatedDecisions.aspx"))

    assert all("ieDecisionDetails" in r["url"] for r in rows)
    assert len(rows) == 2


def test_list_dates_are_normalised_to_iso():
    scraper = make_scraper()
    soup = BeautifulSoup(fixture("modgov_decisions_list.html"), "lxml")

    rows = list(scraper.decision_rows(soup, f"{BASE_URL}/mgDelegatedDecisions.aspx"))

    assert rows[0]["date"] == "2025-04-01"


def test_both_list_pages_are_read_and_deduplicated():
    """
    Councils differ in which of the two pages they populate, and a decision
    can appear on both.
    """
    scraper = make_scraper()
    urls = scraper.list_urls()
    pages = {url: fixture("modgov_decisions_list.html") for url in urls}
    scraper.get_text = lambda url, **kwargs: pages[url]

    rows = list(scraper.get_decisions())

    assert len(urls) == 2
    assert [r["identifier"] for r in rows] == ["13567", "13149"]


def test_a_list_page_a_council_does_not_have_is_not_fatal():
    scraper = make_scraper()
    working, missing = scraper.list_urls()

    def get_text(url, **kwargs):
        if url == missing:
            raise OSError("404")
        return fixture("modgov_decisions_list.html")

    scraper.get_text = get_text

    assert len(list(scraper.get_decisions())) == 2


# ---- the decision page itself ----


def detail_scraper(body, status_code=200, headers=None):
    scraper = make_scraper()
    response = FakeResponse(body, status_code=status_code, headers=headers)
    scraper.get_conditional = lambda url, **kwargs: response
    scraper.validators_for = lambda url: (None, None)
    scraper.response_status = lambda r: r.status_code
    scraper.response_header = lambda r, name: r.headers.get(name)
    scraper.record_source = lambda url, r: {"url": url}
    return scraper


ROW = {
    "url": f"{BASE_URL}/ieDecisionDetails.aspx?ID=1000",
    "identifier": "1000",
    "title": "Minutes of Previous Meeting",
    "date": "2015-10-22",
}


def test_reads_the_decision_text():
    """The text is the point of the record, and is what lands in git."""
    scraper = detail_scraper(fixture("modgov_decision_detail.html"))

    decision = scraper.get_single_decision(ROW)

    assert decision.text == (
        "The Minutes of the Policy Committee meeting on 23 September 2015 were "
        "approved as a correct record."
    )


def test_word_hard_wrapping_is_not_kept_in_the_text():
    """
    Decision text is pasted from Word and hard-wrapped in the source. Read
    straight, every few words gets a newline mid-sentence, which then lands
    in git that way.
    """
    scraper = detail_scraper(fixture("modgov_decision_detail.html"))

    text = scraper.get_single_decision(ROW).text

    assert "\n" not in text
    assert "Policy Committee meeting" in text


def test_paragraph_breaks_are_kept():
    scraper = detail_scraper(fixture("modgov_decision_multi_paragraph.html"))

    text = scraper.get_single_decision(ROW).text

    assert text.split("\n\n") == [
        "The demolition of the existing tower blocks has been approved to "
        "support the regeneration of Berry Brow.",
        "This decision was authorised by the Cabinet at its meeting on "
        "Tuesday, 27 July 2021.",
    ]


def test_spacing_only_paragraphs_are_dropped():
    """Word exports put an &nbsp; paragraph between real ones."""
    scraper = detail_scraper(fixture("modgov_decision_multi_paragraph.html"))

    assert "\n\n\n" not in scraper.get_single_decision(ROW).text


def test_reads_the_labelled_fields():
    scraper = detail_scraper(fixture("modgov_decision_detail.html"))

    decision = scraper.get_single_decision(ROW)

    assert decision.status == "Recommendations Approved"
    assert decision.is_key_decision is False
    assert decision.is_subject_to_call_in is False
    assert decision.publication_date == "2015-10-22"


def test_prefers_the_date_of_decision_over_the_published_date():
    """
    The list page carries the publication date; the decision was made
    earlier, and that is the date the record is filed under.
    """
    scraper = detail_scraper(fixture("modgov_decision_detail.html"))

    decision = scraper.get_single_decision(ROW)

    assert decision.date == "2015-10-19"
    assert decision.publication_date == "2015-10-22"


def test_finds_accompanying_documents():
    scraper = detail_scraper(fixture("modgov_decision_detail.html"))

    decision = scraper.get_single_decision(ROW)

    assert len(decision.documents) == 1
    document = decision.documents[0]
    assert document["url"] == f"{BASE_URL}/documents/s7942/polc2 - 23.09.2015.pdf"
    assert document["attachment_id"] == "s7942"


def test_the_file_size_label_is_not_part_of_the_document_title():
    scraper = detail_scraper(fixture("modgov_decision_detail.html"))

    decision = scraper.get_single_decision(ROW)

    assert decision.documents[0]["title"] == "polc2 - 23.09.2015"


def test_a_decision_with_no_documents_is_fine():
    scraper = detail_scraper(fixture("modgov_decision_detail_no_documents.html"))

    decision = scraper.get_single_decision(ROW)

    assert decision.documents == []


def test_reads_purpose_and_decision_as_separate_sections():
    scraper = detail_scraper(fixture("modgov_decision_detail_no_documents.html"))

    decision = scraper.get_single_decision(ROW)

    assert (
        decision.purpose == "To seek approval for alternative processing arrangements."
    )
    assert decision.text.startswith("That approval be given")


def test_decision_maker_label_varies_between_installs():
    """
    Some pages carry "Decision Maker:", others only "Decided at meeting:".
    Both are the same field as far as the record is concerned.
    """
    with_maker = detail_scraper(fixture("modgov_decision_detail_no_documents.html"))
    with_meeting = detail_scraper(fixture("modgov_decision_detail.html"))

    assert with_maker.get_single_decision(ROW).decision_maker == (
        "Strategic Director for Environment"
    )
    assert "Policy Committee" in with_meeting.get_single_decision(ROW).decision_maker


def test_an_unchanged_decision_is_not_re_scraped():
    scraper = detail_scraper("", status_code=304)

    with pytest.raises(DecisionNotModifiedException):
        scraper.get_single_decision(ROW)


# ---- the request the scraper makes ----


def test_the_window_is_a_year_back_from_today():
    scraper = make_scraper()
    start, end = scraper.date_range

    assert end == datetime.date.today()
    assert (end - start).days == 365


def test_asks_for_published_decisions_over_the_window():
    scraper = make_scraper()
    delegated, officer = scraper.list_urls()
    start, end = scraper.date_range

    assert "mgDelegatedDecisions.aspx" in delegated
    assert "DS=2" in delegated
    assert "mgListOfficerDecisions.aspx" in officer
    assert f"DR={scraper.format_date(start)}-{scraper.format_date(end)}" in delegated


def test_dates_are_slash_encoded_the_way_moderngov_wants_them():
    """The DR parameter takes dd/mm/yyyy with the slashes percent-encoded."""
    assert make_scraper().format_date(datetime.date(2026, 1, 5)) == "05%2f01%2f2026"
