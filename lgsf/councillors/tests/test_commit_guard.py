"""
A councillors run that fails commits nothing.

Downstream readers treat each commit as the whole truth about a council,
and a councillor missing from one as having left. So a run that raises
partway, or that finds implausibly few councillors, must leave whatever
the last good run committed exactly as it was. Driven against the real
local storage backend in a temporary directory.
"""

import pytest

from lgsf.conf import settings
from lgsf.councillors.scrapers import BaseCouncillorScraper
from lgsf.scrapers.base import ScraperBase
from lgsf.storage.backends.base import StorageMode
from lgsf.storage.backends.local import LocalFilesystemStorage


class FakeConsole:
    def log(self, *args, **kwargs):
        pass


class ListedCouncillorScraper(BaseCouncillorScraper):
    """Councillors from a list; raises when asked to, instead of fetching."""

    names: list = []
    fail_at: str | None = None

    def get_councillors(self):
        return [{"name": name} for name in self.names]

    def get_single_councillor(self, row):
        name = row["name"]
        if name == self.fail_at:
            raise OSError(f"site fell over at {name}")
        return self.add_councillor(
            f"https://example.gov.uk/councillors/{name}",
            identifier=name,
            name=name.title(),
            party="Independent",
            division="Ward",
        )


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    monkeypatch.delenv("LGSF_STORAGE_BACKEND", raising=False)
    return tmp_path


@pytest.fixture
def make_scraper(monkeypatch):
    def base_init(self, options, console):
        self.options = options
        self.console = console
        self.council_id = options["council"]
        self.base_url = "https://example.gov.uk"
        self.run_id = options.get("run_id") or "run-test"
        self.execution_id = options.get("execution_id")
        self.storage_backend = LocalFilesystemStorage(
            council_code=self.council_id,
            scraper_object_type="Councillors",
            storage_mode=StorageMode.REPLACE,
        )
        self.storage_session = self.storage_backend.start_session()

    monkeypatch.setattr(ScraperBase, "__init__", base_init)

    def build(names, fail_at=None, **options):
        cls = type(
            "Scraper", (ListedCouncillorScraper,), {"names": names, "fail_at": fail_at}
        )
        return cls({"council": "TST", "verbose": False, **options}, FakeConsole())

    return build


def stored(data_dir):
    """The identifiers of the councillors on disk; files are named <identifier>-<name>."""
    root = data_dir / "TST" / "Councillors" / "json"
    if not root.exists():
        return []
    return sorted(p.stem.rsplit("-", 2)[0] for p in root.iterdir())


FULL_COUNCIL = [f"member-{i:02d}" for i in range(12)]


def run_cli_style(scraper):
    """How manage.py runs a scraper: inside its context manager."""
    with scraper:
        scraper.run(run_log=None)


def test_a_full_run_commits_every_councillor(make_scraper, data_dir):
    run_cli_style(make_scraper(FULL_COUNCIL))

    assert stored(data_dir) == FULL_COUNCIL


def test_too_few_councillors_fails_the_run_and_commits_nothing(make_scraper, data_dir):
    run_cli_style(make_scraper(FULL_COUNCIL))

    with pytest.raises(ValueError, match="Not many councillors found \\(3\\)"):
        run_cli_style(make_scraper(FULL_COUNCIL[:3]))

    assert stored(data_dir) == FULL_COUNCIL


def test_a_run_that_raises_partway_commits_nothing(make_scraper, data_dir):
    run_cli_style(make_scraper(FULL_COUNCIL))
    changed = FULL_COUNCIL[:6] + [
        "newcomer-a",
        "newcomer-b",
        "newcomer-c",
        "newcomer-d",
        "newcomer-e",
        "newcomer-f",
    ]

    with pytest.raises(OSError, match="site fell over"):
        run_cli_style(make_scraper(changed, fail_at="newcomer-d"))

    assert stored(data_dir) == FULL_COUNCIL


def test_the_check_runs_before_the_commit_not_after(make_scraper, data_dir):
    """finalize_storage itself refuses, so no caller can commit first and check later."""
    scraper = make_scraper(FULL_COUNCIL[:3])
    for row in scraper.get_councillors():
        scraper.process_councillor(scraper.get_single_councillor(row), row)

    with pytest.raises(ValueError):
        scraper.finalize_storage()

    assert scraper.storage_session is None
    assert stored(data_dir) == []


def test_a_good_run_after_a_failed_one_replaces_the_set(make_scraper, data_dir):
    run_cli_style(make_scraper(FULL_COUNCIL))
    with pytest.raises(OSError):
        run_cli_style(make_scraper(FULL_COUNCIL, fail_at="member-05"))
    smaller = FULL_COUNCIL[:10]

    run_cli_style(make_scraper(smaller))

    assert stored(data_dir) == smaller


def test_commit_messages_name_the_run_in_trailers(make_scraper):
    scraper = make_scraper(
        FULL_COUNCIL, run_id="abc123", execution_id="scraper-all-20261023"
    )

    assert scraper.commit_message("Updated TST councillors") == (
        "Updated TST councillors\n"
        "\n"
        "LGSF-Run-Id: abc123\n"
        "LGSF-Execution-Id: scraper-all-20261023\n"
        "LGSF-Scraper-Type: Councillors\n"
    )
    assert "LGSF-Execution-Id" not in make_scraper(FULL_COUNCIL).commit_message("x")
