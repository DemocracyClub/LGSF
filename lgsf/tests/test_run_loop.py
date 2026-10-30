"""
How a per-council command runs many councils.

Councils are independent sites, so they are scraped concurrently, and one
of them failing has to leave the rest of the run alone.
"""

import io
import json

import pytest

from lgsf.commands.base import PerCouncilCommandBase
from lgsf.conf import settings


class FakeStorageBackend:
    """Only the part the run log needs: where this council's data lives."""

    def __init__(self, root, council_id):
        self.council_root = root / council_id / "Fake"


class FakeScraper:
    """Stands in for a council's scraper class."""

    disabled = False
    run_on_aws = True
    tags = []
    get_tags = []
    scraper_object_type = "Fake"
    failing_councils = set()
    data_root = None
    run_id = "run-1"
    last_commit = None
    ran = []
    committed = []

    def __init__(self, options, console):
        self.options = options
        self.console = console
        self.council_id = options["council"]
        self.storage_backend = FakeStorageBackend(
            FakeScraper.data_root, self.council_id
        )
        self.storage_session = object()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *args):
        # As the real scraper does: a session still open on a clean exit
        # is committed.
        if self.storage_session and exc_type is None:
            FakeScraper.committed.append(self.council_id)
        return False

    def abandon_storage(self):
        self.storage_session = None

    def run_since(self, hours=24):
        return None

    def run(self, run_log):
        council = self.options["council"]
        FakeScraper.ran.append(council)
        if council in FakeScraper.failing_councils:
            raise RuntimeError(f"{council} is broken")


class Command(PerCouncilCommandBase):
    command_name = "minutes"

    def output_report(self):
        pass


@pytest.fixture
def command(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "BASE_PATH", tmp_path)
    monkeypatch.setattr(
        "lgsf.commands.base.load_scraper", lambda code, cmd: FakeScraper
    )
    monkeypatch.setattr("lgsf.commands.base.load_council_info", lambda code: {})
    FakeScraper.ran = []
    FakeScraper.committed = []
    FakeScraper.failing_councils = set()
    FakeScraper.data_root = tmp_path / "data"

    def make(councils, **options):
        cmd = Command(["minutes", "--council", ",".join(councils)], io.StringIO())
        cmd.options = {
            "council": ",".join(councils),
            "all_councils": False,
            "refresh": False,
            "tags": None,
            "exclude_missing": False,
            "verbose": False,
            "report": False,
            "workers": 4,
            **options,
        }
        return cmd

    return make


def run_logs(tmp_path):
    """Every runlog.json written under the fake data directory."""
    return {
        path.parent.parent.name: json.loads(path.read_text())
        for path in (tmp_path / "data").glob("*/Fake/runlog.json")
    }


def test_every_council_runs_even_when_one_fails(command, tmp_path):
    """The whole point: a broken council must not end the run."""
    cmd = command(["AAA", "BBB", "CCC"])
    FakeScraper.failing_councils = {"BBB"}

    cmd.run_councils()

    assert sorted(FakeScraper.ran) == ["AAA", "BBB", "CCC"]


def test_a_council_that_raises_commits_nothing(command):
    """
    What it stored before raising is partial, and committing it would read
    downstream as everything else having gone.
    """
    cmd = command(["AAA", "BBB"])
    FakeScraper.failing_councils = {"BBB"}

    cmd.run_councils()

    assert FakeScraper.committed == ["AAA"]


def test_a_failure_does_not_abort_even_with_verbose(command):
    """-v is for detail, not for stopping at the first problem."""
    cmd = command(["AAA", "BBB", "CCC"], verbose=True)
    FakeScraper.failing_councils = {"AAA"}

    cmd.run_councils()

    assert sorted(FakeScraper.ran) == ["AAA", "BBB", "CCC"]


def test_a_single_council_still_raises_with_verbose(command):
    """
    One council is someone working on that scraper, or a scripted call that
    needs the failure to reach its exit code.
    """
    cmd = command(["AAA"], verbose=True)
    FakeScraper.failing_councils = {"AAA"}

    with pytest.raises(RuntimeError, match="AAA is broken"):
        cmd.run_councils()


def test_a_single_council_without_verbose_does_not_raise(command):
    cmd = command(["AAA"])
    FakeScraper.failing_councils = {"AAA"}

    cmd.run_councils()

    assert FakeScraper.ran == ["AAA"]


def test_each_council_gets_its_own_options(command):
    """
    Sharing one options dict means concurrent runs overwrite each other's
    council, and scrapers end up scraping the wrong site.
    """
    seen = []
    original = FakeScraper.run

    def record(self, run_log):
        seen.append(self.options["council"])
        return original(self, run_log)

    FakeScraper.run = record
    try:
        cmd = command(["AAA", "BBB", "CCC"])
        cmd.run_councils()
    finally:
        FakeScraper.run = original

    assert sorted(seen) == ["AAA", "BBB", "CCC"]
    # The command's own options are left as the user gave them
    assert cmd.options["council"] == "AAA,BBB,CCC"


class KeptOffAWS(FakeScraper):
    run_on_aws = False


@pytest.fixture
def bbb_kept_off_aws(monkeypatch):
    monkeypatch.setattr(
        "lgsf.commands.base.load_scraper",
        lambda code, cmd: KeptOffAWS if code == "BBB" else FakeScraper,
    )


@pytest.mark.parametrize(
    "run_on_aws, expected",
    [(None, ["AAA", "BBB", "CCC"]), (True, ["AAA", "CCC"]), (False, ["BBB"])],
)
def test_run_on_aws_selects_councils(command, bbb_kept_off_aws, run_on_aws, expected):
    """
    --run-on-aws is what AWS runs; --no-aws is what's left to run from
    elsewhere. Neither runs everything.
    """
    cmd = command(["AAA", "BBB", "CCC"], run_on_aws=run_on_aws)

    cmd.run_councils()

    assert sorted(FakeScraper.ran) == expected


def test_run_on_aws_flags_parse():
    def parsed(*flags):
        cmd = Command(["minutes", "--all-councils", *flags], io.StringIO())
        return cmd.create_parser().run_on_aws

    assert parsed() is None
    assert parsed("--run-on-aws") is True
    assert parsed("--no-aws") is False
    with pytest.raises(SystemExit):
        parsed("--run-on-aws", "--no-aws")


def test_run_log_records_every_council(command, tmp_path):
    cmd = command(["AAA", "BBB"])
    FakeScraper.failing_councils = {"BBB"}

    cmd.run_councils()

    entries = run_logs(tmp_path)
    assert sorted(entries) == ["AAA", "BBB"]
    assert entries["AAA"]["status_code"] == 0
    assert entries["BBB"]["status_code"] == 1
    assert "BBB is broken" in entries["BBB"]["error"]
    assert entries["AAA"]["command"] == "minutes"


def test_run_log_is_not_written_in_lambda(command, tmp_path):
    """Lambda runs one council per invocation and logs to CloudWatch."""
    cmd = command(["AAA"], aws_lambda=True)

    cmd.run_councils()

    assert run_logs(tmp_path) == {}


def test_run_log_sits_beside_the_data_it_describes(command, tmp_path):
    cmd = command(["AAA"])

    cmd.run_councils()

    assert (tmp_path / "data" / "AAA" / "Fake" / "runlog.json").is_file()


def test_only_the_most_recent_run_is_kept(command, tmp_path):
    """One file per council and type, replaced each run."""
    command(["AAA"]).run_councils()
    FakeScraper.failing_councils = {"AAA"}
    command(["AAA"]).run_councils()

    entry = run_logs(tmp_path)["AAA"]
    assert entry["status_code"] == 1
    assert "AAA is broken" in entry["error"]


def test_a_backend_with_nowhere_local_to_write_is_skipped(command, tmp_path):
    """The GitHub backend keeps data elsewhere and reports via the dashboard."""

    class Remote:
        pass

    original = FakeScraper.__init__

    def no_local_root(self, options, console):
        original(self, options, console)
        self.storage_backend = Remote()

    FakeScraper.__init__ = no_local_root
    try:
        command(["AAA"]).run_councils()
    finally:
        FakeScraper.__init__ = original

    assert run_logs(tmp_path) == {}


def test_workers_can_be_forced_to_one(command):
    cmd = command(["AAA", "BBB"], workers=1)

    cmd.run_councils()

    assert cmd._concurrent is False
    assert sorted(FakeScraper.ran) == ["AAA", "BBB"]


def test_a_single_council_is_never_run_concurrently(command):
    """Its output should stream rather than be replayed at the end."""
    cmd = command(["AAA"], workers=8)

    cmd.run_councils()

    assert cmd._concurrent is False


def test_results_file_gets_each_councils_result_as_aws_reports_it(command, tmp_path):
    results = tmp_path / "results.jsonl"
    cmd = command(["AAA", "BBB"], results_file=str(results))
    FakeScraper.failing_councils = {"BBB"}

    cmd.run_councils()

    by_council = {
        r["council"]: r for r in map(json.loads, results.read_text().splitlines())
    }
    assert by_council["AAA"]["status"] == "completed"
    assert by_council["AAA"]["run_id"] == "run-1"
    assert by_council["BBB"]["status"] == "failed"
    assert by_council["BBB"]["error"] == "RuntimeError: BBB is broken"
