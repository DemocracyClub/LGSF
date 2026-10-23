"""
Rewriting a repository's history so every commit keeps councillor files
under Councillors/, against bare repositories on disk standing in for the
LGSF-Data ones. The fixtures replay the three layouts the real
repositories went through: flat Councillors/x.json in 2021, then
Councillors/json/, then json/ at the root with pull-request merges.
"""

import csv
import subprocess

import pytest

from scripts.migrate_github_layout import OLD_PATH, migrate_repo, run, write_report


@pytest.fixture(autouse=True)
def isolated_git(monkeypatch):
    """Keep the developer's commit signing, hooks and templates out of the test commits."""
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for var in ("GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME"):
        monkeypatch.setenv(var, "Scraper")
    for var in ("GIT_AUTHOR_EMAIL", "GIT_COMMITTER_EMAIL"):
        monkeypatch.setenv(var, "scraper@example.com")


def git(*args, cwd):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout


class Repo:
    """A bare repository and a working clone to build its history with."""

    def __init__(self, tmp_path, name):
        self.bare = tmp_path / f"{name}.git"
        git(
            "init",
            "--bare",
            "--quiet",
            "--initial-branch=main",
            str(self.bare),
            cwd=tmp_path,
        )
        self.work = tmp_path / f"{name}-work"
        git("clone", "--quiet", str(self.bare), str(self.work), cwd=tmp_path)

    def commit(self, message, files, remove=()):
        """Write files, remove paths, commit on the current branch."""
        for path in remove:
            git("rm", "-r", "--quiet", path, cwd=self.work)
        for path, content in files.items():
            (self.work / path).parent.mkdir(parents=True, exist_ok=True)
            (self.work / path).write_text(content)
        git("add", "-A", cwd=self.work)
        git("commit", "--quiet", "--allow-empty", "-m", message, cwd=self.work)
        return git("rev-parse", "HEAD", cwd=self.work).strip()

    def scrape_via_pull_request(self, date, files, remove=()):
        """What the Lambda did: a branch, a commit, a merge commit on main."""
        branch = f"ABC-{date}-run"
        git("checkout", "--quiet", "-b", branch, cwd=self.work)
        self.commit("Updated ABC", files, remove)
        git("checkout", "--quiet", "main", cwd=self.work)
        git(
            "merge",
            "--quiet",
            "--no-ff",
            "-m",
            f"Merge ABC data ({date})",
            branch,
            cwd=self.work,
        )
        git("branch", "--quiet", "-D", branch, cwd=self.work)

    def refresh(self):
        """Clone the bare repository again, as the Lambda does before every run."""
        subprocess.run(["rm", "-rf", str(self.work)], check=True)
        git("clone", "--quiet", str(self.bare), str(self.work), cwd=self.work.parent)

    def push(self, *refs):
        git("push", "--quiet", "--force", "origin", *(refs or ("main",)), cwd=self.work)

    # --- reading the bare repository back ---

    def commits(self, ref="main"):
        return git("rev-list", ref, cwd=self.bare).split()

    def tree(self, ref="main"):
        return sorted(git("ls-tree", "-r", "--name-only", ref, cwd=self.bare).split())

    def every_path_ever(self, ref="--all"):
        return set(git("log", ref, "--format=", "--name-only", cwd=self.bare).split())

    def blobs(self, ref="main"):
        return {
            line.split()[2]
            for line in git("ls-tree", "-r", ref, cwd=self.bare).splitlines()
        }

    def branches(self):
        return sorted(
            git(
                "for-each-ref",
                "--format=%(refname:short)",
                "refs/heads/",
                cwd=self.bare,
            ).split()
        )


@pytest.fixture
def realistic(tmp_path):
    """Flat files in 2021, Councillors/json/ until 2025, then root json/ via PRs."""
    repo = Repo(tmp_path, "ABC")
    repo.commit(
        "Scraped on 2021-07-06",
        {
            "Councillors/1-alice.json": '{"name": "Alice"}',
            "Councillors/2-bob.json": '{"name": "Bob"}',
        },
    )
    repo.commit(
        "Councillors - scraped on 2022-03-07",
        {
            "Councillors/json/1-alice.json": '{"name": "Alice"}',
            "Councillors/json/2-bob.json": '{"name": "Bob"}',
            "Councillors/raw/1-alice.html": "<p>Alice</p>",
            "Councillors/raw/2-bob.html": "<p>Bob</p>",
        },
        remove=["Councillors/1-alice.json", "Councillors/2-bob.json"],
    )
    repo.commit("Initial commit", {"README.md": "# Data for ABC\n"})
    repo.scrape_via_pull_request(
        "2025-10-21",
        {
            "json/1-alice.json": '{"name": "Alice"}',
            "json/2-bob.json": '{"name": "Bob"}',
            "raw/1-alice.html": "<p>Alice</p>",
            "raw/2-bob.html": "<p>Bob</p>",
        },
        remove=["Councillors"],
    )
    # Bob leaves, Carol arrives
    repo.scrape_via_pull_request(
        "2026-01-15",
        {
            "json/3-carol.json": '{"name": "Carol"}',
            "raw/3-carol.html": "<p>Carol</p>",
        },
        remove=["json/2-bob.json", "raw/2-bob.html"],
    )
    repo.push()
    return repo


def migrate(repo, tmp_path, **kwargs):
    kwargs.setdefault("dry_run", False)
    return migrate_repo("ABC", str(repo.bare), tmp_path / "work", **kwargs)


def test_every_commit_ends_up_in_the_new_layout(realistic, tmp_path):
    blobs_before = realistic.blobs()
    old_main = realistic.commits()[0]

    result = migrate(realistic, tmp_path)

    assert result.action == "rewritten", result.error
    assert (result.old_main, result.new_main) == (old_main, realistic.commits()[0])
    assert result.new_main != old_main
    assert not {p for p in realistic.every_path_ever() if OLD_PATH.match(p.encode())}
    assert realistic.tree() == [
        "Councillors/json/1-alice.json",
        "Councillors/json/3-carol.json",
        "Councillors/raw/1-alice.html",
        "Councillors/raw/3-carol.html",
        "README.md",
    ]
    assert realistic.blobs() == blobs_before


def test_each_commit_is_a_snapshot_at_the_same_paths(realistic, tmp_path):
    """Walking main oldest to newest, a councillor's file never moves."""
    migrate(realistic, tmp_path)

    first, *_, last = reversed(realistic.commits())
    assert realistic.tree(first) == [
        "Councillors/json/1-alice.json",
        "Councillors/json/2-bob.json",
    ]
    assert "Councillors/json/2-bob.json" not in realistic.tree(last)
    bob_in = [
        c
        for c in realistic.commits()
        if "Councillors/json/2-bob.json" in realistic.tree(c)
    ]
    assert (
        len(bob_in) == len(realistic.commits()) - 2
    )  # gone in the last scrape and its merge


def test_history_shape_is_kept(realistic, tmp_path):
    merges_before = git("rev-list", "--merges", "--count", "main", cwd=realistic.bare)
    messages_before = git("log", "--format=%s", "main", cwd=realistic.bare)

    result = migrate(realistic, tmp_path)

    assert (
        git("rev-list", "--merges", "--count", "main", cwd=realistic.bare)
        == merges_before
    )
    assert git("log", "--format=%s", "main", cwd=realistic.bare) == messages_before
    assert result.commits_after == result.commits_before
    assert (
        git("log", "-1", "--format=%an", "main", cwd=realistic.bare).strip()
        == "Scraper"
    )


def test_dry_run_rewrites_and_checks_locally_but_pushes_nothing(realistic, tmp_path):
    before = realistic.commits()

    result = migrate(realistic, tmp_path, dry_run=True)

    assert result.action == "rewritten"
    assert result.new_main and result.new_main != result.old_main
    assert realistic.commits() == before


def test_already_migrated_repository_is_left_alone(realistic, tmp_path):
    migrate(realistic, tmp_path)
    after_first = realistic.commits()

    result = migrate(realistic, tmp_path)

    assert result.action == "already-migrated"
    assert result.new_main == result.old_main == after_first[0]
    assert realistic.commits() == after_first


def test_a_commit_holding_both_layouts_is_refused(tmp_path):
    """A scrape with the new backend ran before the rewrite: don't guess which json/ wins."""
    repo = Repo(tmp_path, "ABC")
    repo.commit("Updated ABC", {"json/1-alice.json": '{"v": 1}'})
    repo.commit("Updated ABC", {"Councillors/json/1-alice.json": '{"v": 2}'})
    repo.push()
    before = repo.commits()

    result = migrate(repo, tmp_path)

    assert result.action == "failed"
    assert "root json/" in result.error
    assert repo.commits() == before


def test_an_old_code_scrape_after_the_rewrite_is_refused_too(realistic, tmp_path):
    """The old backend never deleted Councillors/, so its commit holds both layouts."""
    migrate(realistic, tmp_path)
    realistic.refresh()
    realistic.scrape_via_pull_request("2026-03-01", {"json/5-eve.json": "{}"})
    realistic.push("main")
    before = realistic.commits()

    result = migrate(realistic, tmp_path)

    assert result.action == "failed"
    assert "root json/" in result.error
    assert realistic.commits() == before


def with_stray_branches(repo):
    git("checkout", "--quiet", "-b", "ABC-2026-02-04-orphan", cwd=repo.work)
    repo.commit("Updated ABC", {"json/4-dan.json": "{}"})
    git("checkout", "--quiet", "main", cwd=repo.work)
    git("branch", "--quiet", "backup-original-main", "main", cwd=repo.work)
    repo.push("main", "ABC-2026-02-04-orphan", "backup-original-main")


def test_other_branches_are_rewritten_by_default(realistic, tmp_path):
    with_stray_branches(realistic)

    result = migrate(realistic, tmp_path)

    assert result.action == "rewritten", result.error
    assert result.branches_deleted == 0
    assert realistic.branches() == [
        "ABC-2026-02-04-orphan",
        "backup-original-main",
        "main",
    ]
    assert "Councillors/json/4-dan.json" in realistic.tree("ABC-2026-02-04-orphan")
    assert not {p for p in realistic.every_path_ever() if OLD_PATH.match(p.encode())}


def test_other_branches_are_deleted_on_request(realistic, tmp_path):
    with_stray_branches(realistic)

    result = migrate(realistic, tmp_path, delete_other_branches=True)

    assert result.action == "rewritten", result.error
    assert result.branches_deleted == 2
    assert realistic.branches() == ["main"]


def test_push_fails_if_main_moved_since_the_clone(realistic, tmp_path, monkeypatch):
    import scripts.migrate_github_layout as module

    real_git = module.git

    def git_then_race(args, cwd, check=True):
        if args[0] == "filter-repo":
            realistic.refresh()
            realistic.scrape_via_pull_request("2026-04-01", {"json/6-fay.json": "{}"})
            realistic.push("main")
        return real_git(args, cwd, check)

    monkeypatch.setattr(module, "git", git_then_race)
    raced_main = None

    result = migrate(realistic, tmp_path)

    raced_main = realistic.commits()[0]
    assert result.action == "failed"
    assert "push" in result.error
    assert "json/6-fay.json" in realistic.tree(raced_main)


def test_repository_with_no_commits_is_reported_as_empty(tmp_path):
    repo = Repo(tmp_path, "NEW")

    result = migrate_repo("NEW", str(repo.bare), tmp_path / "work", dry_run=False)

    assert result.action == "empty"


def test_unreachable_repository_is_a_failure_not_an_exception(tmp_path):
    result = migrate_repo(
        "GONE", str(tmp_path / "missing.git"), tmp_path / "work", dry_run=False
    )

    assert result.action == "failed"
    assert result.error


def test_run_handles_many_repositories_and_writes_a_report(realistic, tmp_path):
    done = Repo(tmp_path, "DEF")
    done.commit("Updated DEF", {"README.md": "x", "Councillors/json/c.json": "{}"})
    done.push()
    bares = {"ABC": realistic.bare, "DEF": done.bare, "NEW": Repo(tmp_path, "NEW").bare}

    results = run(
        sorted(bares),
        lambda r: str(bares[r]),
        tmp_path / "work",
        dry_run=False,
        workers=2,
    )
    report = tmp_path / "report.csv"
    write_report(results, report)

    assert [(r.repo, r.action) for r in results] == [
        ("ABC", "rewritten"),
        ("DEF", "already-migrated"),
        ("NEW", "empty"),
    ]
    rows = list(csv.DictReader(report.open()))
    assert [row["repo"] for row in rows] == ["ABC", "DEF", "NEW"]
    assert rows[0]["old_main"] != rows[0]["new_main"]
    assert rows[0]["commits_before"] == rows[0]["commits_after"] != "0"
