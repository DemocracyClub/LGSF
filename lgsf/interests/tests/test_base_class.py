from unittest.mock import MagicMock

from lgsf.interests.scrapers import BaseInterestsScraper
from lgsf.storage.backends.base import StorageMode


def test_base_interests_scraper_attributes():
    options = {"council": "CBF"}
    console = MagicMock()

    scraper = BaseInterestsScraper(options, console)
    assert scraper.scraper_object_type == "Interests"
    assert scraper.service_name == "interests"
    assert scraper.storage_mode == StorageMode.REPLACE
    assert scraper.report_items == []

    record = scraper.add_interests_register(
        url="https://example.gov.uk",
        identifier="1",
        councillor_name="Cllr Test",
        councillor_id="1",
    )
    assert record in scraper.interests_registers
    assert len(scraper.report_items) == 1
