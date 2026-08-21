"""
Tests for `manage.py metadata update`.

A single council is fetched from the organisation detail endpoint, which
answers for a known identifier directly. That it stays a single request,
and that an unknown identifier is reported rather than passed on as an
update, are both pinned down here.
"""

import io

import pytest

from lgsf.metadata.commands import Command


DETAIL_URL = (
    "https://elections.democracyclub.org.uk/api/organisations/local-authority/{}/"
)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def make_command():
    return Command(["metadata"], io.StringIO(), pretty=False)


def org(identifier, org_type="local-authority"):
    return {
        "official_identifier": identifier,
        "organisation_type": org_type,
        "slug": identifier.lower(),
    }


@pytest.fixture
def detail_api(monkeypatch):
    """The detail endpoint, answering for one known council."""
    responses = {
        DETAIL_URL.format("KIR"): {"count": 1, "next": None, "results": [org("KIR")]},
        DETAIL_URL.format("ZZZ"): {"count": 0, "next": None, "results": []},
    }
    requested = []

    def fake_get(url, **kwargs):
        requested.append((url, kwargs))
        return FakeResponse(responses[url])

    monkeypatch.setattr("lgsf.metadata.commands.requests.get", fake_get)
    return requested


def test_fetches_a_council_in_a_single_request(detail_api, monkeypatch):
    command = make_command()
    updated = []
    monkeypatch.setattr(command, "update_council_metadata", updated.append)

    command.update_single_council("KIR")

    assert [o["official_identifier"] for o in updated] == ["KIR"]
    assert len(detail_api) == 1


def test_asks_the_local_authority_detail_endpoint(detail_api, monkeypatch):
    """The org type is scoped by the URL, not filtered out afterwards."""
    command = make_command()
    monkeypatch.setattr(command, "update_council_metadata", lambda org: None)

    command.update_single_council("KIR")

    assert detail_api[0][0] == DETAIL_URL.format("KIR")


def test_reports_a_council_that_is_genuinely_absent(detail_api, monkeypatch, capsys):
    """An unknown identifier is an empty result set, not a 404."""
    command = make_command()
    monkeypatch.setattr(
        command, "update_council_metadata", lambda org: pytest.fail("should not update")
    )

    command.update_single_council("ZZZ")

    assert "ZZZ not found" in capsys.readouterr().out
    assert len(detail_api) == 1


class TestAutoUpdateScraperInfo:
    """
    base_url is filled in from a council's scraper files, for every
    service that has one.
    """

    def scraper(self, base_url):
        return (
            "from lgsf.x import Y\n\n\nclass Scraper(Y):\n"
            f'    base_url = "{base_url}"\n'
        )

    def test_fills_in_base_url_for_every_service_with_a_scraper(self, tmp_path):
        from lgsf.metadata.models import CouncilMetadata

        (tmp_path / "councillors.py").write_text(self.scraper("https://a.gov.uk"))
        (tmp_path / "minutes.py").write_text(self.scraper("https://b.gov.uk"))
        (tmp_path / "__init__.py").write_text("")
        metadata = CouncilMetadata()

        make_command().auto_update_scraper_info(metadata, tmp_path)

        assert metadata.get_service_metadata("councillors").base_url == (
            "https://a.gov.uk"
        )
        assert metadata.get_service_metadata("minutes").base_url == "https://b.gov.uk"

    def test_works_for_a_service_the_framework_has_never_heard_of(self, tmp_path):
        from lgsf.metadata.models import CouncilMetadata

        (tmp_path / "decisions.py").write_text(self.scraper("https://c.gov.uk"))
        metadata = CouncilMetadata()

        make_command().auto_update_scraper_info(metadata, tmp_path)

        assert metadata.get_service_metadata("decisions").base_url == "https://c.gov.uk"

    def test_does_not_overwrite_a_base_url_already_recorded(self, tmp_path):
        from lgsf.metadata.models import CouncilMetadata

        (tmp_path / "minutes.py").write_text(self.scraper("https://scraper.gov.uk"))
        metadata = CouncilMetadata()
        metadata.update_service_data("minutes", base_url="https://metadata.gov.uk")

        make_command().auto_update_scraper_info(metadata, tmp_path)

        assert metadata.get_service_metadata("minutes").base_url == (
            "https://metadata.gov.uk"
        )

    def test_ignores_dunder_files(self, tmp_path):
        from lgsf.metadata.models import CouncilMetadata

        (tmp_path / "__init__.py").write_text(self.scraper("https://nope.gov.uk"))
        metadata = CouncilMetadata()

        make_command().auto_update_scraper_info(metadata, tmp_path)

        assert metadata.configured_services() == []

    def test_ignores_a_commented_out_base_url(self, tmp_path):
        """Reading the parsed assignment, not the text, so comments don't count."""
        from lgsf.metadata.models import CouncilMetadata

        (tmp_path / "minutes.py").write_text(
            "from lgsf.x import Y\n\n\nclass Scraper(Y):\n"
            '    # base_url = "https://old.gov.uk"\n'
            '    base_url = "https://current.gov.uk"\n'
        )
        metadata = CouncilMetadata()

        make_command().auto_update_scraper_info(metadata, tmp_path)

        assert metadata.get_service_metadata("minutes").base_url == (
            "https://current.gov.uk"
        )

    def test_ignores_a_base_url_that_is_not_a_literal(self, tmp_path):
        """A URL built at runtime can't be recorded, so it must not be guessed at."""
        from lgsf.metadata.models import CouncilMetadata

        (tmp_path / "minutes.py").write_text(
            "from lgsf.x import Y\n\n\nclass Scraper(Y):\n"
            "    base_url = SOME_CONSTANT + '/path'\n"
        )
        metadata = CouncilMetadata()

        make_command().auto_update_scraper_info(metadata, tmp_path)

        assert metadata.get_service_metadata("minutes").base_url is None

    def test_survives_a_scraper_that_does_not_parse(self, tmp_path, capsys):
        """A half-written scraper warns rather than taking the command down."""
        from lgsf.metadata.models import CouncilMetadata

        (tmp_path / "minutes.py").write_text("class Scraper(:\n")
        metadata = CouncilMetadata()

        make_command().auto_update_scraper_info(metadata, tmp_path)

        assert "could not parse" in capsys.readouterr().out
        assert metadata.get_service_metadata("minutes").base_url is None


def test_service_name_defaults_to_councillors():
    command = make_command()
    command.options = {}
    assert command.service_name == "councillors"


def test_service_name_follows_the_service_option():
    command = make_command()
    command.options = {"service": "minutes"}
    assert command.service_name == "minutes"
