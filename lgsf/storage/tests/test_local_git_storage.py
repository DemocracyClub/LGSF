"""
The git backend: production's GitHub path against bare repositories here.

It shares the GitHub session's code, so these mostly check the seams the
local version replaces (making and probing the repository) and that a
scraper driven end to end leaves on main exactly what production would.
"""

import subprocess
from pathlib import Path

import pytest

from lgsf.councillors.scrapers import BaseCouncillorScraper
from lgsf.scrapers.base import ScraperBase
from lgsf.storage.backends import get_available_backends, get_storage_backend
from lgsf.storage.backends.base import StorageMode
from lgsf.storage.backends.git import LocalGitStorage


@pytest.fixture(autouse=True)
def git_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("LGSF_STORAGE_BACKEND", "git")
    monkeypatch.setenv("LGSF_GIT_REMOTE", str(tmp_path / "repos"))
    monkeypatch.setenv("LGSF_GIT_WORKDIR", str(tmp_path / "work"))
    return tmp_path


def git(*args, cwd):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout


def bare(tmp_path, council="ABC"):
    return tmp_path / "repos" / f"{council}.git"


def tree(repo, ref="main"):
    return sorted(git("ls-tree", "-r", "--name-only", ref, cwd=repo).split())


def log(repo):
    return git("log", "--format=%s", "main", cwd=repo).split("\n")[:-1]


def storage(mode=StorageMode.REPLACE):
    return get_storage_backend(
        "ABC",
        options={"storage_backend": "git"},
        scraper_object_type="Councillors",
        storage_mode=mode,
    )


MESSAGE = (
    "Updated ABC councillors\n\nLGSF-Run-Id: run-1\nLGSF-Scraper-Type: Councillors\n"
)


def test_the_backend_is_selectable_and_needs_a_remote_directory(monkeypatch):
    assert "git" in get_available_backends()
    assert isinstance(storage(), LocalGitStorage)
    monkeypatch.delenv("LGSF_GIT_REMOTE")
    with pytest.raises(ValueError, match="LGSF_GIT_REMOTE"):
        storage()


def test_a_first_run_makes_the_repository_and_commits_like_production(git_environment):
    store = storage()
    session = store.start_session(run_id="run-1")
    session.write(Path("json/a.json"), "{}")
    session.write(Path("raw/a.html"), "<p>")

    result = store.end_session(session, MESSAGE)

    repo = bare(git_environment)
    assert tree(repo) == [
        "Councillors/json/a.json",
        "Councillors/raw/a.html",
        "README.md",
    ]
    assert log(repo) == ["Updated ABC councillors", "Initial commit"]
    assert git("rev-parse", "main", cwd=repo).strip() == result["commit"]
    assert git("log", "-1", "--format=%B", "main", cwd=repo).strip() == MESSAGE.strip()
    assert git("log", "-1", "--format=%an", "main", cwd=repo).strip() == "LGSF Bot"


def test_runs_replace_skip_and_abort_as_on_github(git_environment):
    repo = bare(git_environment)
    with storage().session(MESSAGE, run_id="run-1") as session:
        session.write(Path("json/departed.json"), "{}")
    with storage().session(MESSAGE, run_id="run-2") as session:
        session.write(Path("json/current.json"), "{}")
    assert tree(repo) == ["Councillors/json/current.json", "README.md"]
    before = git("rev-parse", "main", cwd=repo)

    store = storage()
    session = store.start_session(run_id="run-3")
    session.write(Path("json/current.json"), "{}")
    assert store.end_session(session, MESSAGE)["skipped"]
    assert git("rev-parse", "main", cwd=repo) == before

    store = storage()
    session = store.start_session(run_id="run-4")
    session.write(Path("json/partial.json"), "{}")
    store.abort_session(session)
    assert git("rev-parse", "main", cwd=repo) == before
    assert not Path(session.local_repo_path).exists()


def test_other_types_in_the_same_repository_are_left_alone(git_environment):
    with storage(StorageMode.ACCUMULATE).session(MESSAGE, run_id="m") as session:
        pass
    minutes = get_storage_backend(
        "ABC",
        options={"storage_backend": "git"},
        scraper_object_type="Minutes",
        storage_mode=StorageMode.ACCUMULATE,
    )
    with minutes.session("Minutes", run_id="m1") as session:
        session.write(Path("json/2026-01-01.json"), "{}")
    with storage().session(MESSAGE, run_id="c1") as session:
        session.write(Path("json/a.json"), "{}")

    assert tree(bare(git_environment)) == [
        "Councillors/json/a.json",
        "Minutes/json/2026-01-01.json",
        "README.md",
    ]


# --- a real scraper, end to end ---


class FakeConsole:
    def log(self, *args, **kwargs):
        pass


class ListedCouncillorScraper(BaseCouncillorScraper):
    names: list = []

    def get_councillors(self):
        return [{"name": name} for name in self.names]

    def get_single_councillor(self, row):
        return self.add_councillor(
            f"https://example.gov.uk/councillors/{row['name']}",
            identifier=row["name"],
            name=row["name"].title(),
            party="Independent",
            division="Ward",
        )


@pytest.fixture
def make_scraper(monkeypatch):
    def base_init(self, options, console):
        self.options = options
        self.console = console
        self.council_id = options["council"]
        self.base_url = "https://example.gov.uk"
        self.run_id = options.get("run_id", "run-42")
        self.execution_id = options.get("execution_id")
        self.storage_backend = get_storage_backend(
            self.council_id,
            options=options,
            scraper_object_type="Councillors",
            storage_mode=StorageMode.REPLACE,
        )
        self.storage_session = self.storage_backend.start_session(run_id=self.run_id)

    monkeypatch.setattr(ScraperBase, "__init__", base_init)

    def build(names, **options):
        cls = type("Scraper", (ListedCouncillorScraper,), {"names": names})
        return cls({"council": "TST", "verbose": False, **options}, FakeConsole())

    return build


FULL_COUNCIL = [f"member-{i:02d}" for i in range(12)]


def test_a_scraper_run_leaves_on_main_what_production_would(
    git_environment, make_scraper
):
    scraper = make_scraper(FULL_COUNCIL, execution_id="laptop-2026-10-23")
    with scraper:
        scraper.run(run_log=None)

    repo = bare(git_environment, "TST")
    assert git("rev-parse", "main", cwd=repo).strip() == scraper.last_commit
    assert log(repo) == ["Updated TST councillors", "Initial commit"]
    assert git("log", "-1", "--format=%(trailers:only)", "main", cwd=repo).split("\n")[
        :3
    ] == [
        "LGSF-Run-Id: run-42",
        "LGSF-Execution-Id: laptop-2026-10-23",
        "LGSF-Scraper-Type: Councillors",
    ]
    assert len([p for p in tree(repo) if p.startswith("Councillors/json/")]) == 12


def test_a_scraper_run_that_fails_its_check_commits_nothing_here_either(
    git_environment, make_scraper
):
    with make_scraper(FULL_COUNCIL) as scraper:
        scraper.run(run_log=None)
    repo = bare(git_environment, "TST")
    before = git("rev-parse", "main", cwd=repo)

    with pytest.raises(ValueError, match="Not many councillors"):
        with make_scraper(FULL_COUNCIL[:3]) as scraper:
            scraper.run(run_log=None)

    assert git("rev-parse", "main", cwd=repo) == before
