from unittest.mock import MagicMock

from lgsf.interests.scrapers import ModGovInterestsScraper


SAMPLE_XML = """<?xml version="1.0" encoding="utf-8"?>
<councillorsbyward>
  <wards>
    <ward>
      <wardtitle>Central</wardtitle>
      <councillors>
        <councillor>
          <councillorid>100</councillorid>
          <fullusername>Cllr Sample Member</fullusername>
        </councillor>
      </councillors>
    </ward>
  </wards>
</councillorsbyward>
"""

SAMPLE_HTML = """
<html>
  <body>
    <h1>Register of interests</h1>
    <p>This register of interests was published on Tuesday, 10 March 2026, 11.00 a.m..</p>
    <table class="mgInterestsTable" summary="Employment">
      <caption>1. Employment</caption>
      <thead>
        <tr><th>Me</th><th>Partner</th></tr>
      </thead>
      <tbody>
        <tr><td>Accountant</td><td>None</td></tr>
      </tbody>
    </table>
    <a href="/forms/declaration.pdf">Download PDF Declaration</a>
  </body>
</html>
"""

SAMPLE_USERINFO_PDF = """
<html>
  <body>
    <h1>Cllr Sample Member</h1>
    <a href="mgConvert2PDF.aspx?ID=100&T=6">Register of interests PDF 8 KB</a>
  </body>
</html>
"""


def test_modgov_interests_scraper_html_parsing(monkeypatch):
    options = {"council": "CBF"}
    console = MagicMock()

    scraper = ModGovInterestsScraper(options, console)
    scraper.base_url = "https://example.gov.uk"

    def mock_get_text(url, **kwargs):
        if "GetCouncillorsByWard" in url:
            return SAMPLE_XML
        return SAMPLE_HTML

    monkeypatch.setattr(scraper, "get_text", mock_get_text)

    wards = scraper.get_interests_registers()
    assert len(wards) == 1
    ward = wards[0]
    councillor_xml = ward.find("councillor")

    record, raw = scraper.get_single_interests_register(ward, councillor_xml)

    assert record.identifier == "100"
    assert record.councillor_name == "Cllr Sample Member"
    assert record.division == "Central"
    assert record.published_date == "Tuesday, 10 March 2026, 11.00 a.m"
    assert len(record.interests) == 1
    assert record.interests[0]["category"] == "1. Employment"
    assert record.interests[0]["headers"] == ["Me", "Partner"]
    assert record.interests[0]["rows"] == [["Accountant", "None"]]
    assert len(record.documents) == 1
    assert record.documents[0]["url"] == "https://example.gov.uk/forms/declaration.pdf"


def test_modgov_interests_scraper_pdf_parsing(monkeypatch):
    options = {"council": "ABD"}
    console = MagicMock()

    scraper = ModGovInterestsScraper(options, console)
    scraper.base_url = "https://example.gov.uk"

    def mock_get_text(url, **kwargs):
        if "GetCouncillorsByWard" in url:
            return SAMPLE_XML
        if "mgUserInfo.aspx" in url:
            return SAMPLE_USERINFO_PDF
        return ""

    monkeypatch.setattr(scraper, "get_text", mock_get_text)

    wards = scraper.get_interests_registers()
    ward = wards[0]
    councillor_xml = ward.find("councillor")

    record, raw = scraper.get_single_interests_register(ward, councillor_xml)

    assert record.identifier == "100"
    assert record.councillor_name == "Cllr Sample Member"
    assert len(record.interests) == 0
    assert len(record.documents) == 1
    assert record.documents[0]["url"] == "https://example.gov.uk/mgConvert2PDF.aspx?ID=100&T=6"
    assert "mgConvert2PDF" in record.url
