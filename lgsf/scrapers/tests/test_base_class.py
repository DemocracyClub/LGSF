import pytest

from lgsf.councillors.scrapers import BaseCouncillorScraper
from lgsf.scrapers.base import ScraperBase


def test_abc_raises():
    with pytest.raises(TypeError) as excinfo:
        BaseCouncillorScraper(options={})
    assert "Can't instantiate abstract class BaseCouncillorScraper" in str(
        excinfo.value
    )


class ContentResponse:
    """requests and httpx: the body is .content."""

    content = b"%PDF-1.7"


class BytesResponse:
    """wreq: the body is .bytes()."""

    def bytes(self):
        return b"%PDF-1.7"


def scraper():
    return ScraperBase.__new__(ScraperBase)


@pytest.mark.parametrize("response", [ContentResponse(), BytesResponse()])
def test_response_bytes_reads_the_body_from_any_client(response):
    assert scraper().response_bytes(response) == b"%PDF-1.7"


def test_response_bytes_refuses_a_response_with_no_body():
    with pytest.raises(ValueError, match="Can't read bytes"):
        scraper().response_bytes(object())


def test_get_bytes_fetches_and_reads_the_body():
    instance = scraper()
    requested = []
    instance.get = lambda url: requested.append(url) or BytesResponse()

    assert instance.get_bytes("https://ex.gov.uk/a.pdf") == b"%PDF-1.7"
    assert requested == ["https://ex.gov.uk/a.pdf"]
