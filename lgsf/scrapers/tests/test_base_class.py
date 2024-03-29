import pytest

from lgsf.councillors.scrapers import BaseCouncillorScraper


def test_abc_raises():
    with pytest.raises(TypeError) as excinfo:
        BaseCouncillorScraper(options={})
    assert "Can't instantiate abstract class BaseCouncillorScraper" in str(
        excinfo.value
    )
