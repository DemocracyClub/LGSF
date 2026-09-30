from pathlib import Path
from unittest.mock import MagicMock

from lgsf.interests.scrapers import BaseInterestsScraper, ModGovInterestsScraper

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name):
    return (FIXTURES / name).read_text()


def test_modgov_interests_scraper_html_parsing(monkeypatch):
    options = {"council": "CBF"}
    console = MagicMock()

    scraper = ModGovInterestsScraper(options, console)
    scraper.base_url = "https://example.gov.uk"

    def mock_get_text(url, **kwargs):
        if "GetCouncillorsByWard" in url:
            return fixture("modgov_councillors.xml")
        return fixture("modgov_rofi_html.html")

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
    assert record.interests[0]["rows"] == [{"Me": "Accountant", "Partner": "None"}]
    assert len(record.documents) == 1
    assert record.documents[0]["url"] == "https://example.gov.uk/forms/declaration.pdf"


def test_modgov_interests_scraper_pdf_parsing(monkeypatch):
    options = {"council": "ABD"}
    console = MagicMock()

    scraper = ModGovInterestsScraper(options, console)
    scraper.base_url = "https://example.gov.uk"

    def mock_get_text(url, **kwargs):
        if "GetCouncillorsByWard" in url:
            return fixture("modgov_councillors.xml")
        if "mgUserInfo.aspx" in url:
            return fixture("modgov_userinfo_pdf.html")
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
    assert (
        record.documents[0]["url"]
        == "https://example.gov.uk/mgConvert2PDF.aspx?ID=100&T=6"
    )
    assert "mgConvert2PDF" in record.url


def test_save_interest_documents_skips_existing():
    """
    Documents already in the store must not be re-downloaded, but their
    storage metadata (storage_key etc.) must still be backfilled via
    describe() so the JSON looks the same as a freshly downloaded document.
    """
    scraper = BaseInterestsScraper.__new__(BaseInterestsScraper)
    scraper.options = {}
    scraper.console = MagicMock()
    scraper.documents_downloaded = 0
    scraper.documents_skipped = 0
    scraper.documents_linked = 0

    fake_stored = MagicMock()
    fake_stored.as_dict.return_value = {
        "storage_key": "42-cllr-test-1.pdf",
        "storage_backend": "local",
        "content_hash": "sha256:abc",
        "content_length": 1024,
    }
    fake_doc_storage = MagicMock()
    fake_doc_storage.exists.return_value = True
    fake_doc_storage.describe.return_value = fake_stored
    scraper.document_storage = fake_doc_storage

    from lgsf.interests.models import RegisterOfInterestsBase

    record = RegisterOfInterestsBase.__new__(RegisterOfInterestsBase)
    record.councillor_id = "42"
    record.councillor_name = "Cllr Test"
    record.documents = [{"url": "https://example.gov.uk/form.pdf", "title": "Form"}]

    scraper.save_interest_documents(record)

    # Must not download
    fake_doc_storage.write.assert_not_called()
    # Must call describe() to recover stored metadata
    fake_doc_storage.describe.assert_called_once()
    # Storage metadata must be merged into the document dict
    assert record.documents[0]["storage_key"] == "42-cllr-test-1.pdf"
    assert record.documents[0]["content_hash"] == "sha256:abc"
    assert scraper.documents_skipped == 1
    assert scraper.documents_downloaded == 0
    assert scraper.documents_linked == 1


def test_save_interest_documents_downloads_new():
    """
    New documents (not yet in the store) are fetched and written.
    """
    scraper = BaseInterestsScraper.__new__(BaseInterestsScraper)
    scraper.options = {}
    scraper.console = MagicMock()
    scraper.documents_downloaded = 0
    scraper.documents_skipped = 0
    scraper.documents_linked = 0

    fake_stored = MagicMock()
    fake_stored.as_dict.return_value = {
        "storage_key": "42-cllr-test-1.pdf",
        "content_hash": "abc",
    }
    fake_doc_storage = MagicMock()
    fake_doc_storage.exists.return_value = False
    fake_doc_storage.write.return_value = fake_stored
    scraper.document_storage = fake_doc_storage
    scraper.get_bytes = MagicMock(return_value=b"%PDF-1.4 content")

    from lgsf.interests.models import RegisterOfInterestsBase

    record = RegisterOfInterestsBase.__new__(RegisterOfInterestsBase)
    record.councillor_id = "42"
    record.councillor_name = "Cllr Test"
    record.documents = [{"url": "https://example.gov.uk/form.pdf", "title": "Form"}]

    scraper.save_interest_documents(record)

    fake_doc_storage.write.assert_called_once()
    assert scraper.documents_downloaded == 1
    assert scraper.documents_skipped == 0
