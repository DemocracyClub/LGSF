"""
Scraper validation across services.

Validation takes a service name, so a whole data type can be checked in
one pass: that a council declaring a service has a matching scraper file,
that base_url is set, and that the scraper's base class agrees with the
recorded cms_type.
"""

import json

import pytest

from lgsf.conf import settings
from lgsf.metadata.validation import ScraperValidator


@pytest.fixture
def council(tmp_path, monkeypatch):
    """
    A council directory the validator will resolve against.

    path_utils._abs_path resolves SCRAPER_DIR_NAME relative to the working
    directory and ignores BASE_PATH, so point it at an absolute temporary
    path rather than trying to relocate the base.
    """
    scrapers_dir = tmp_path / "scrapers"
    path = scrapers_dir / "ZZQ-anytown"
    path.mkdir(parents=True)
    monkeypatch.setattr(settings, "SCRAPER_DIR_NAME", str(scrapers_dir))

    def write(scrapers=None, services=None):
        for name, body in (scrapers or {}).items():
            (path / f"{name}.py").write_text(body)
        (path / "metadata.json").write_text(
            json.dumps(
                {
                    "everyelectiion_data": {"official_identifier": "ZZQ"},
                    "services": services or {},
                }
            )
        )
        return path

    return write


MODGOV_MINUTES = (
    "from lgsf.minutes.scrapers import ModGovMinutesScraper\n\n\n"
    "class Scraper(ModGovMinutesScraper):\n    pass\n"
)
MODGOV_COUNCILLORS = (
    "from lgsf.councillors.scrapers import ModGovCouncillorScraper\n\n\n"
    "class Scraper(ModGovCouncillorScraper):\n    pass\n"
)
SERVICE = {"base_url": "https://example.gov.uk", "cms_type": "ModernGov"}


def test_a_valid_minutes_scraper_passes(council):
    council(scrapers={"minutes": MODGOV_MINUTES}, services={"minutes": SERVICE})

    report = ScraperValidator().validate_council_scraper("ZZQ", "minutes")

    assert report["valid"] is True
    assert report["errors"] == []
    assert report["service"] == "minutes"


def test_a_council_without_the_service_is_not_applicable(council):
    """Most councils will never have every service; that isn't a failure."""
    council(
        scrapers={"councillors": MODGOV_COUNCILLORS},
        services={"councillors": SERVICE},
    )

    report = ScraperValidator().validate_council_scraper("ZZQ", "minutes")

    assert report["applicable"] is False
    assert report["errors"] == []


def test_metadata_without_a_scraper_file_is_an_error(council):
    council(services={"minutes": SERVICE})

    report = ScraperValidator().validate_council_scraper("ZZQ", "minutes")

    assert report["valid"] is False
    assert "no minutes.py" in report["errors"][0]


def test_a_scraper_file_without_metadata_is_an_error(council):
    council(scrapers={"minutes": MODGOV_MINUTES}, services={})

    report = ScraperValidator().validate_council_scraper("ZZQ", "minutes")

    assert report["valid"] is False
    assert "services.minutes" in report["errors"][0]


def test_a_missing_base_url_is_an_error_naming_the_right_service(council):
    council(
        scrapers={"minutes": MODGOV_MINUTES},
        services={"minutes": {"cms_type": "ModernGov"}},
    )

    report = ScraperValidator().validate_council_scraper("ZZQ", "minutes")

    assert report["valid"] is False
    assert any("services.minutes.base_url" in e for e in report["errors"])


def test_services_are_validated_independently(council):
    """A broken minutes scraper must not affect the councillors verdict."""
    council(
        scrapers={"councillors": MODGOV_COUNCILLORS},
        services={"councillors": SERVICE, "minutes": SERVICE},
    )
    validator = ScraperValidator()

    assert validator.validate_council_scraper("ZZQ", "councillors")["valid"] is True
    assert validator.validate_council_scraper("ZZQ", "minutes")["valid"] is False


def test_a_bare_cms_subclass_is_not_flagged_as_incomplete(council):
    """
    Subclassing a CMS base class and adding nothing is the expected shape:
    the base class does the work and the council supplies a base_url.
    """
    council(scrapers={"minutes": MODGOV_MINUTES}, services={"minutes": SERVICE})

    report = ScraperValidator().validate_council_scraper("ZZQ", "minutes")

    assert not any("No custom methods" in w for w in report["warnings"])


def test_a_bare_non_cms_subclass_is_flagged(council):
    council(
        scrapers={
            "minutes": "class Scraper(BaseMinutesScraper):\n    pass\n",
        },
        services={"minutes": {"base_url": "https://example.gov.uk"}},
    )

    report = ScraperValidator().validate_council_scraper("ZZQ", "minutes")

    assert any("No custom methods" in w for w in report["warnings"])


@pytest.mark.parametrize(
    "parent_class,expected",
    [
        ("ModGovMinutesScraper", "ModernGov"),
        ("ModGovCouncillorScraper", "ModernGov"),
        ("CMISMinutesScraper", "CMIS"),
        ("CMISCouncillorScraper", "CMIS"),
        # PagedHTML must win over HTML - it is checked first
        ("PagedHTMLCouncillorScraper", "Custom HTML (Paged)"),
        ("HTMLCouncillorScraper", "Custom HTML"),
        ("JSONCouncillorScraper", "JSON API"),
        ("BaseCouncillorScraper", "Custom Base"),
        ("SomethingElse", None),
        (None, None),
    ],
)
def test_expected_cms_type_is_read_from_the_base_class_name(parent_class, expected):
    """
    Base classes are named <CMS><Type>Scraper, so matching the prefix means
    a new scraper type needs no entry in the table.
    """
    assert ScraperValidator()._expected_cms_type(parent_class) == expected


def test_cms_mismatch_is_reported(council):
    council(
        scrapers={"minutes": MODGOV_MINUTES},
        services={
            "minutes": {"base_url": "https://example.gov.uk", "cms_type": "CMIS"}
        },
    )

    report = ScraperValidator().validate_council_scraper("ZZQ", "minutes")

    assert any("CMS type mismatch" in w for w in report["warnings"])
