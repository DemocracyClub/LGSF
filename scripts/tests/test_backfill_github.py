"""
Seeding council repositories with records already on S3: the fake S3
client the storage tests use, and bare repositories on disk standing in
for the LGSF-Data ones.
"""

import csv
import json
import subprocess

import pytest

from lgsf.storage.tests.test_s3_storage import FakeS3
from scripts.backfill_github import Backfill, is_record, write_report


@pytest.fixture(autouse=True)
def isolated_git(monkeypatch):
    """Keep the developer's commit signing, hooks and templates out of the test commits."""
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")


def git(*args, cwd):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout


def bare_repo(tmp_path, council, files):
    """A repository whose main holds a README and the given files."""
    bare = tmp_path / "repos" / f"{council}.git"
    bare.parent.mkdir(exist_ok=True)
    git("init", "--bare", "--quiet", "--initial-branch=main", str(bare), cwd=tmp_path)
    seed = tmp_path / f"seed-{council}"
    git("clone", "--quiet", str(bare), str(seed), cwd=tmp_path)
    for path, content in {"README.md": "# Data\n", **files}.items():
        (seed / path).parent.mkdir(parents=True, exist_ok=True)
        (seed / path).write_text(content)
    git("add", "-A", cwd=seed)
    git(
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.com",
        "commit",
        "--quiet",
        "-m",
        "Updated",
        cwd=seed,
    )
    git("push", "--quiet", "origin", "HEAD:main", cwd=seed)
    return bare


def tree(bare):
    return sorted(git("ls-tree", "-r", "--name-only", "main", cwd=bare).split())


def log(bare):
    return git("log", "--format=%s", "main", cwd=bare).splitlines()


def put(fake, key, body):
    fake.objects[key] = {"Body": body.encode() if isinstance(body, str) else body}


DECISION = json.dumps(
    {"raw_title": "Budget", "documents": [{"storage_key": "ab12.pdf"}]}
)
INDEX = json.dumps(
    {"decisions": {"u": {"file_name": "2024-01-01-1.json"}}, "documents": {}}
)


@pytest.fixture
def fake():
    fake = FakeS3()
    for council in ("ABD", "KIR"):
        put(fake, f"merged-data/{council}/Decisions/json/2024-01-01-1.json", DECISION)
        put(
            fake,
            f"merged-data/{council}/Decisions/raw/2024-01-01-1.html",
            "<p>Budget</p>",
        )
        put(fake, f"merged-data/{council}/Decisions/_index.json", INDEX)
        put(fake, f"merged-data/{council}/Decisions/_last-run", "2026-10-01T00:00:00")
        put(fake, f"merged-data/{council}/Decisions/runlog.json", "{}")
        put(fake, f"merged-data/{council}/Decisions/scrape_summary.json", "{}")
        put(fake, f"merged-data/{council}/documents/ab12.pdf", b"%PDF")
    return fake


def backfill(fake, tmp_path, **kwargs):
    kwargs.setdefault("scraper_types", ["decisions"])
    return Backfill(
        fake,
        "lgsf-data-archive",
        "merged-data",
        remote_for=lambda council: str(tmp_path / "repos" / f"{council}.git"),
        workdir=tmp_path / "work",
        workers=2,
        out=lambda *a: None,
        **kwargs,
    )


def test_one_commit_per_council_adds_the_records(fake, tmp_path):
    repo = bare_repo(tmp_path, "ABD", {"Councillors/json/a.json": "{}"})

    result = backfill(fake, tmp_path).run_council("ABD")

    assert (result.action, result.commits, result.files) == ("committed", 1, 4)
    assert tree(repo) == [
        "Councillors/json/a.json",
        "Decisions/_index.json",
        "Decisions/_last-run",
        "Decisions/json/2024-01-01-1.json",
        "Decisions/raw/2024-01-01-1.html",
        "README.md",
    ]
    assert git("show", "main:Decisions/json/2024-01-01-1.json", cwd=repo) == DECISION
    assert log(repo) == ["Backfill ABD decisions from S3", "Updated"]


def test_the_commit_is_the_bots_and_says_where_it_came_from(fake, tmp_path):
    repo = bare_repo(tmp_path, "ABD", {})

    backfill(fake, tmp_path).run_council("ABD")

    assert git("log", "-1", "--format=%an", "main", cwd=repo).strip() == "LGSF Bot"
    trailers = git("log", "-1", "--format=%(trailers:only)", "main", cwd=repo)
    assert "LGSF-Scraper-Type: Decisions" in trailers
    assert (
        "LGSF-Backfill: s3://lgsf-data-archive/merged-data/ABD/Decisions/" in trailers
    )


def test_documents_and_run_artefacts_are_not_copied(fake, tmp_path):
    repo = bare_repo(tmp_path, "ABD", {})

    backfill(fake, tmp_path).run_council("ABD")

    assert not [
        p for p in tree(repo) if "documents" in p or "runlog" in p or "summary" in p
    ]
    assert not is_record("documents/ab12.pdf")
    assert is_record("_index.json") and is_record("json/a.json")


def test_files_already_in_the_repository_are_kept_and_s3_wins_on_the_same_path(
    fake, tmp_path
):
    repo = bare_repo(
        tmp_path,
        "ABD",
        {
            "Decisions/json/2026-10-20-9.json": '{"new": true}',
            "Decisions/json/2024-01-01-1.json": "stale",
        },
    )

    backfill(fake, tmp_path).run_council("ABD")

    assert "Decisions/json/2026-10-20-9.json" in tree(repo)
    assert git("show", "main:Decisions/json/2024-01-01-1.json", cwd=repo) == DECISION


def test_a_rerun_makes_no_commit(fake, tmp_path):
    repo = bare_repo(tmp_path, "ABD", {})
    backfill(fake, tmp_path).run_council("ABD")
    before = log(repo)

    result = backfill(fake, tmp_path).run_council("ABD")

    assert result.action == "unchanged"
    assert log(repo) == before


def test_a_dry_run_reads_nothing_and_commits_nothing(fake, tmp_path):
    repo = bare_repo(tmp_path, "ABD", {})
    fake.calls.clear()

    result = backfill(fake, tmp_path, dry_run=True).run_council("ABD")

    assert (result.action, result.files) == ("would-commit", 4)
    assert not [op for op, _ in fake.calls if op == "get"]
    assert log(repo) == ["Updated"]


def test_a_council_with_no_repository_is_skipped(fake, tmp_path):
    result = backfill(fake, tmp_path).run_council("KIR")
    assert result.action == "no-repository"


def test_a_council_with_nothing_on_s3_is_skipped(fake, tmp_path):
    bare_repo(tmp_path, "XYZ", {})
    assert backfill(fake, tmp_path).run_council("XYZ").action == "nothing-on-s3"


def test_main_moving_during_the_backfill_is_redone(fake, tmp_path, monkeypatch):
    """The daily run pushed between our clone and our push: redo on a fresh clone."""
    import scripts.backfill_github as module

    repo = bare_repo(tmp_path, "ABD", {})
    real_git = module.git
    raced = []

    def git_then_race(args, cwd, check=True):
        if args[0] == "push" and not raced:
            raced.append(1)
            other = tmp_path / "racer"
            git("clone", "--quiet", str(repo), str(other), cwd=tmp_path)
            (other / "Councillors").mkdir()
            (other / "Councillors" / "new.json").write_text("{}")
            git("add", "-A", cwd=other)
            git(
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@example.com",
                "commit",
                "--quiet",
                "-m",
                "Updated ABD councillors",
                cwd=other,
            )
            git("push", "--quiet", "origin", "HEAD:main", cwd=other)
        return real_git(args, cwd, check)

    monkeypatch.setattr(module, "git", git_then_race)

    result = backfill(fake, tmp_path).run_council("ABD")

    assert result.action == "committed"
    assert log(repo) == [
        "Backfill ABD decisions from S3",
        "Updated ABD councillors",
        "Updated",
    ]
    assert "Councillors/new.json" in tree(repo)


def test_councillors_are_refused(fake, tmp_path):
    with pytest.raises(ValueError, match="replace everything"):
        backfill(fake, tmp_path, scraper_types=["councillors"])


def test_every_council_under_the_prefix_and_a_report(fake, tmp_path):
    bare_repo(tmp_path, "ABD", {})
    b = backfill(fake, tmp_path)

    results = b.run(b.councils(), workers=2)
    report = tmp_path / "report.csv"
    write_report(results, report)

    assert [(r.council, r.action) for r in results] == [
        ("ABD", "committed"),
        ("KIR", "no-repository"),
    ]
    rows = list(csv.DictReader(report.open()))
    assert rows[0]["files"] == "4" and rows[0]["head"]
