"""
The enumerator lists each council with its scraper types, in run order.

The state machine runs a council's types one at a time in the order given
here, so the order and the grouping are what matter. Most tests replace
each type's command with a fake that names its councils; one runs the real
commands against the real scrapers directory.
"""

from types import SimpleNamespace

import pytest

from lgsf import scraper_types
from lgsf.aws_lambda import handlers


class FakeCouncil:
    def __init__(self, council_id, current=True):
        self.council_id = council_id
        self.current = current


@pytest.fixture
def councils_by_type(monkeypatch):
    """Make each type's command choose the councils listed for it."""

    class Listed(dict):
        built: list

    listed = Listed()
    built = []

    def fake_import(path):
        scraper_type = path.split(".")[1]

        class Command:
            def __init__(self, argv, stdout):
                built.append((scraper_type, argv))

        Command.listed = listed.get(scraper_type, [])
        return SimpleNamespace(Command=Command)

    def councils_to_run(command):
        return type(command).listed

    monkeypatch.setattr(handlers, "import_module", fake_import)
    monkeypatch.setattr(
        handlers.PerCouncilCommandBase,
        "councils_to_run",
        property(councils_to_run),
    )
    listed.built = built
    return listed


@pytest.fixture
def registry(monkeypatch):
    def set_registry(**types):
        monkeypatch.setattr(scraper_types, "SCRAPER_TYPES", types)

    return set_registry


def enumerate_(event):
    return handlers.council_enumerator_handler(event, None)


ALL_DAILY = {
    "councillors": {"daily": True},
    "minutes": {"daily": True},
    "decisions": {"daily": True},
}


def test_each_council_gets_its_types_in_registry_order(councils_by_type, registry):
    registry(**ALL_DAILY)
    councils_by_type["councillors"] = [FakeCouncil("ABC"), FakeCouncil("KIR")]
    councils_by_type["minutes"] = [FakeCouncil("KIR")]
    councils_by_type["decisions"] = [FakeCouncil("ABC"), FakeCouncil("KIR")]

    result = enumerate_({})

    assert result["statusCode"] == 200
    assert result["councils"] == [
        {"council": "ABC", "scraper_types": ["councillors", "decisions"]},
        {"council": "KIR", "scraper_types": ["councillors", "minutes", "decisions"]},
    ]
    assert (result["total_councils"], result["total_runs"]) == (2, 5)


def test_by_default_only_daily_types_run(councils_by_type, registry):
    registry(councillors={"daily": True}, minutes={"daily": False})
    councils_by_type["councillors"] = [FakeCouncil("ABC")]
    councils_by_type["minutes"] = [FakeCouncil("ABC")]

    result = enumerate_({"all_councils": True})

    assert result["councils"] == [{"council": "ABC", "scraper_types": ["councillors"]}]
    assert result["input_params"]["scraper_types"] == ["councillors"]


def test_a_type_that_isnt_daily_runs_when_asked_for(councils_by_type, registry):
    registry(councillors={"daily": True}, minutes={"daily": False})
    councils_by_type["minutes"] = [FakeCouncil("KIR")]

    result = enumerate_({"scraper_types": ["minutes"], "council_ids": "KIR"})

    assert result["councils"] == [{"council": "KIR", "scraper_types": ["minutes"]}]
    assert [t for t, _ in councils_by_type.built] == ["minutes"]


def test_types_named_out_of_order_still_run_in_registry_order(
    councils_by_type, registry
):
    registry(**ALL_DAILY)
    for t in ALL_DAILY:
        councils_by_type[t] = [FakeCouncil("KIR")]

    result = enumerate_({"scraper_types": "decisions,councillors"})

    assert result["councils"] == [
        {"council": "KIR", "scraper_types": ["councillors", "decisions"]}
    ]


def test_an_unknown_type_is_refused(councils_by_type, registry):
    registry(**ALL_DAILY)

    result = enumerate_({"scraper_types": ["minutes", "committees"]})

    assert result["statusCode"] == 400
    assert "committees" in result["error"]
    assert result["councils"] == []
    assert councils_by_type.built == []


def test_a_scheduled_run_drops_councils_that_arent_current(councils_by_type, registry):
    registry(councillors={"daily": True})
    councils_by_type["councillors"] = [
        FakeCouncil("ABC"),
        FakeCouncil("OLD", current=False),
    ]

    assert [c["council"] for c in enumerate_({})["councils"]] == ["ABC"]


def test_named_councils_are_passed_to_each_types_command(councils_by_type, registry):
    registry(**ALL_DAILY)

    enumerate_({"council_ids": "KIR,STO"})

    assert councils_by_type.built == [
        (t, ["", "--council", "KIR,STO"]) for t in ALL_DAILY
    ]


def test_with_the_real_commands_and_scrapers():
    """Every registered type's command can be built and choose councils in the Lambda's way."""
    result = enumerate_(
        {"council_ids": "KIR", "scraper_types": list(scraper_types.SCRAPER_TYPES)}
    )

    assert result["statusCode"] == 200
    assert result["councils"] == [
        {"council": "KIR", "scraper_types": list(scraper_types.SCRAPER_TYPES)}
    ]


def test_councils_kept_off_aws_are_left_out(monkeypatch, registry):
    """A site that blocks AWS is run from elsewhere with --no-aws instead."""
    registry(councillors={"daily": True})

    class OnAWS:
        run_on_aws = True

    class KeptOff:
        run_on_aws = False

    monkeypatch.setattr(
        "lgsf.commands.base.load_scraper",
        lambda code, cmd: KeptOff if code == "STO" else OnAWS,
    )

    result = enumerate_({"council_ids": "KIR,STO"})

    assert result["councils"] == [{"council": "KIR", "scraper_types": ["councillors"]}]
