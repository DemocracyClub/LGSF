"""
The order a run over many councils takes them in.

A backfill outlasts the credentials it runs with, so it is stopped and
resumed. A resume should start on councils that have nothing yet, not list
again the ones it has just finished.
"""

import io

import pytest

from lgsf.commands.base import Council
from lgsf.conf import settings
from lgsf.decisions.commands import Command
from lgsf.decisions.scrapers import ModGovDecisionsScraper


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    monkeypatch.delenv("LGSF_STORAGE_BACKEND", raising=False)
    return tmp_path


@pytest.fixture
def command(data_dir, monkeypatch):
    without_scraper = set()
    monkeypatch.setattr(
        "lgsf.decisions.commands.load_scraper",
        lambda code, cmd: None if code in without_scraper else ModGovDecisionsScraper,
    )
    cmd = Command(["decisions", "--all-councils"], io.StringIO())
    cmd.options = {"refresh": None}
    cmd.without_scraper = without_scraper
    return cmd


def store(data_dir, code, index=True, last_run=None):
    root = data_dir / code / "Decisions"
    root.mkdir(parents=True)
    if index:
        (root / "_index.json").write_text("{}")
    if last_run:
        (root / "_last-run").write_text(last_run)


def order(command, *codes):
    return [c.council_id for c in command.backfill_order([Council(c) for c in codes])]


def test_councils_with_nothing_stored_go_first(command, data_dir):
    store(data_dir, "OLD", last_run="2026-10-01T10:00:00")

    assert order(command, "OLD", "NEW") == ["NEW", "OLD"]


def test_councils_stopped_part_way_come_before_finished_ones(command, data_dir):
    """An index but no completed run is what an interrupted backfill leaves."""
    store(data_dir, "DONE", last_run="2026-10-01T10:00:00")
    store(data_dir, "PART")

    assert order(command, "DONE", "PART") == ["PART", "DONE"]


def test_finished_councils_go_least_recently_run_first(command, data_dir):
    store(data_dir, "LATE", last_run="2026-10-09T09:00:00")
    store(data_dir, "SOON", last_run="2026-10-02T09:00:00")
    store(data_dir, "MID", last_run="2026-10-05T09:00:00")

    assert order(command, "LATE", "SOON", "MID") == ["SOON", "MID", "LATE"]


def test_looking_does_not_create_directories(command, data_dir):
    order(command, "AAA", "BBB")

    assert list(data_dir.iterdir()) == []


def test_councils_without_a_scraper_go_last_unexamined(command, data_dir):
    command.without_scraper.add("NONE")

    assert order(command, "NONE", "AAA") == ["AAA", "NONE"]


def test_a_single_council_is_not_looked_up(command, monkeypatch):
    monkeypatch.setattr(
        "lgsf.commands.base.PerCouncilCommandBase.councils_to_run",
        property(lambda self: [Council("AAA")]),
    )
    monkeypatch.setattr(
        Command, "backfill_order", lambda *args: pytest.fail("looked up")
    )

    assert [c.council_id for c in command.councils_to_run] == ["AAA"]
