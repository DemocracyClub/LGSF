"""
CouncilMetadata service handling.

Services are keyed by name rather than being one field per service, so
adding a scraper type needs no change to the model. The round-trip tests
carry the most weight: a service has to survive load-and-save intact,
including one the model knows nothing specific about.
"""

import json

import pytest

from lgsf.metadata.models import CouncilMetadata, ServiceData


@pytest.fixture
def metadata_file(tmp_path):
    path = tmp_path / "metadata.json"
    path.write_text(
        json.dumps(
            {
                "everyelectiion_data": {
                    "official_identifier": "ABC",
                    "common_name": "Anytown",
                },
                "services": {
                    "councillors": {
                        "base_url": "https://example.gov.uk/councillors",
                        "cms_type": "ModernGov",
                    },
                    "minutes": {
                        "base_url": "https://example.gov.uk/minutes",
                        "cms_type": "CMIS",
                    },
                },
            }
        )
    )
    return path


def test_services_load_from_file(metadata_file):
    metadata = CouncilMetadata.from_file(metadata_file)

    assert metadata.get_service_metadata("councillors").cms_type == "ModernGov"
    assert metadata.get_service_metadata("minutes").cms_type == "CMIS"


def test_an_unconfigured_service_reads_as_empty_not_none(metadata_file):
    """Callers read .base_url directly, so this must not be None."""
    metadata = CouncilMetadata.from_file(metadata_file)

    service = metadata.get_service_metadata("decisions")

    assert isinstance(service, ServiceData)
    assert service.base_url is None


def test_reading_an_unconfigured_service_does_not_create_it(metadata_file):
    metadata = CouncilMetadata.from_file(metadata_file)
    metadata.get_service_metadata("decisions")

    assert "decisions" not in metadata.to_dict()["services"]


def test_a_service_the_model_has_never_heard_of_survives_a_round_trip(tmp_path):
    """
    The point of keying services by name: a type nobody has written model
    code for still loads and saves intact.
    """
    path = tmp_path / "metadata.json"
    path.write_text(
        json.dumps(
            {
                "everyelectiion_data": {"official_identifier": "ABC"},
                "services": {
                    "decisions": {
                        "base_url": "https://example.gov.uk/decisions",
                        "cms_type": "ModernGov",
                    }
                },
            }
        )
    )

    metadata = CouncilMetadata.from_file(path)
    metadata.save_to_file(path)

    reloaded = json.loads(path.read_text())
    assert reloaded["services"]["decisions"] == {
        "base_url": "https://example.gov.uk/decisions",
        "cms_type": "ModernGov",
    }


def test_saving_preserves_every_service(metadata_file):
    metadata = CouncilMetadata.from_file(metadata_file)
    metadata.save_to_file(metadata_file)

    reloaded = json.loads(metadata_file.read_text())
    assert sorted(reloaded["services"]) == ["councillors", "minutes"]


def test_update_service_data_creates_a_new_service(tmp_path):
    metadata = CouncilMetadata()
    metadata.update_service_data("decisions", base_url="https://example.gov.uk")

    assert metadata.get_service_metadata("decisions").base_url == (
        "https://example.gov.uk"
    )
    assert metadata.to_dict()["services"]["decisions"]["base_url"] == (
        "https://example.gov.uk"
    )


def test_update_service_data_updates_an_existing_service(metadata_file):
    metadata = CouncilMetadata.from_file(metadata_file)
    metadata.update_service_data("minutes", base_url="https://new.example.gov.uk")

    service = metadata.get_service_metadata("minutes")
    assert service.base_url == "https://new.example.gov.uk"
    # Fields not mentioned are left alone
    assert service.cms_type == "CMIS"


def test_configured_services_lists_only_populated_ones(metadata_file):
    metadata = CouncilMetadata.from_file(metadata_file)
    metadata.services["decisions"] = ServiceData()

    assert metadata.configured_services() == ["councillors", "minutes"]


def test_has_service(metadata_file):
    metadata = CouncilMetadata.from_file(metadata_file)

    assert metadata.has_service("minutes") is True
    assert metadata.has_service("decisions") is False


def test_services_are_written_in_a_stable_order(tmp_path):
    """Otherwise saving churns the diff for no reason."""
    metadata = CouncilMetadata()
    for name in ("minutes", "councillors", "decisions"):
        metadata.update_service_data(name, base_url=f"https://example.gov.uk/{name}")

    path = tmp_path / "metadata.json"
    metadata.save_to_file(path)

    written = json.loads(path.read_text())
    assert list(written["services"]) == ["councillors", "decisions", "minutes"]


def test_empty_services_block_is_handled(tmp_path):
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps({"everyelectiion_data": {}, "services": {}}))

    metadata = CouncilMetadata.from_file(path)

    assert metadata.configured_services() == []
    assert metadata.get_service_metadata("councillors").base_url is None


def test_missing_file_gives_empty_metadata(tmp_path):
    metadata = CouncilMetadata.from_file(tmp_path / "nope.json")

    assert metadata.configured_services() == []


def test_get_summary_reports_every_service_not_just_councillors(metadata_file):
    """
    A council has a cms_type and base_url per service; there is no "the"
    one. Reporting councillors' as though there were misleads any caller
    for a council whose services differ.
    """
    metadata = CouncilMetadata.from_file(metadata_file)

    summary = metadata.get_summary()

    assert summary["services"] == {
        "councillors": {
            "cms_type": "ModernGov",
            "base_url": "https://example.gov.uk/councillors",
        },
        "minutes": {
            "cms_type": "CMIS",
            "base_url": "https://example.gov.uk/minutes",
        },
    }
    assert "cms_type" not in summary
    assert "base_url" not in summary


def test_get_summary_omits_unconfigured_services(tmp_path):
    metadata = CouncilMetadata()
    metadata.services["decisions"] = ServiceData()
    metadata.update_service_data("minutes", base_url="https://example.gov.uk")

    assert list(metadata.get_summary()["services"]) == ["minutes"]
