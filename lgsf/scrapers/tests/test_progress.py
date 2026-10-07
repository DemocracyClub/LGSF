"""
The live progress display for long local runs, and that nothing else pays
for it.
"""

import io

from rich.console import Console

from lgsf.scrapers import progress as progress_module
from lgsf.scrapers.base import ScraperBase
from lgsf.scrapers.progress import LiveProgress, NullProgress


def render(live):
    console = Console(file=io.StringIO(), width=250, force_terminal=False)
    console.print(live)
    return console.file.getvalue()


def test_scrapers_report_to_nothing_by_default():
    """Lambda and nightly runs have nobody watching and nothing shared."""
    assert isinstance(ScraperBase.progress, NullProgress)
    ScraperBase.progress.set(anything=1)
    ScraperBase.progress.add("anything")
    ScraperBase.progress.doing("anything")


def test_a_running_council_shows_where_it_is():
    live = LiveProgress(total=3)
    kir = live.for_council("KIR")
    kir.set(
        window="2026-01",
        windows_done=1,
        windows_total=26,
        window_decisions=165,
        window_decisions_done=40,
        documents_downloaded=12,
        documents_skipped=3,
        bytes=5 * 1024 * 1024,
    )
    kir.add("requests", 57)
    kir.doing("GET https://democracy.kirklees.gov.uk/ieDecisionDetails.aspx?ID=1")

    out = render(live)

    assert "0/3 councils done" in out
    assert "KIR" in out
    assert "2026-01 (1/26)" in out
    assert "40/165" in out
    assert "12/3/0" in out
    assert "5.0 MB" in out
    assert "57" in out
    assert "ieDecisionDetails" in out


def test_finished_councils_leave_the_table_for_the_totals():
    live = LiveProgress(total=2)
    done = live.for_council("AAA")
    done.set(documents_downloaded=7)
    live.for_council("BBB")

    live.finish("AAA")
    out = render(live)

    assert "1/2 councils done" in out
    assert "7 documents" in out
    assert "AAA" not in out.split("\n", 1)[1]


def test_councils_that_failed_or_gave_up_are_named():
    live = LiveProgress(total=3)
    live.for_council("AAA")
    live.for_council("BBB").set(given_up=True)
    live.finish("AAA", failed=True)
    live.finish("BBB")

    assert "Gave up or failed: AAA BBB" in render(live)


def test_a_request_running_past_its_limit_stands_out(monkeypatch):
    """This is how a stalled worker is told from a busy one."""
    now = [100.0]
    monkeypatch.setattr(progress_module.time, "monotonic", lambda: now[0])
    live = LiveProgress(total=1)
    council = live.for_council("RUT")
    council.doing("GET https://slow.example/", limit=130)
    now[0] += 200

    text = live.activity_text(council, now[0])

    assert text.plain.strip().startswith("200s")
    assert "red" in str(text.style)


def test_live_progress_is_only_for_local_concurrent_runs_in_a_terminal(monkeypatch):
    from lgsf.decisions.commands import Command

    command = Command.__new__(Command)
    command.console = Console(file=io.StringIO(), force_terminal=True)
    command.options = {}
    command._concurrent = True
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
    assert command.use_live_progress

    command._concurrent = False
    assert not command.use_live_progress

    command._concurrent = True
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "scraper-worker")
    assert not command.use_live_progress

    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME")
    command.console = Console(file=io.StringIO(), force_terminal=False)
    assert not command.use_live_progress
