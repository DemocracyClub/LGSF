"""
Where the GitHub backend puts files in a council's repository.

Each data type has its own directory, ``<Type>/``, with ``json/`` and
``raw/`` inside it: the same layout as the local backend's
``data/<COUNCIL>/<Type>/``. reps-wiki reads the repositories as a log, and
treats a councillor missing from a commit as having left, so it matters
that a REPLACE run deletes exactly the files of its own type and nothing
else.

These run the real git plumbing against a bare repository on disk; the
only GitHub API calls on this path are stubbed out. The branch is pushed
but not merged, as in production that step is a pull request.
"""

import subprocess
import uuid
from pathlib import Path

import pytest

from lgsf.storage.backends.base import StorageMode
from lgsf.storage.backends.github import _GitHubSession


@pytest.fixture(autouse=True)
def isolated_git(monkeypatch):
    """
    Keep the developer's own git configuration out of these tests: commit
    signing, hooks and templates would otherwise apply to the test commits.
    """
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")


def git(*args, cwd):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout


@pytest.fixture
def bare_repo(tmp_path):
    """A bare repository whose main holds a README, as a new LGSF-Data repo does."""
    bare = tmp_path / "ABC.git"
    git("init", "--bare", "--quiet", "--initial-branch=main", str(bare), cwd=tmp_path)
    seed = tmp_path / "seed"
    git("clone", "--quiet", str(bare), str(seed), cwd=tmp_path)
    (seed / "README.md").write_text("# Data for ABC\n")
    git("add", "README.md", cwd=seed)
    git(
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.com",
        "commit",
        "--quiet",
        "-m",
        "Initial commit",
        cwd=seed,
    )
    git("push", "--quiet", "origin", "HEAD:main", cwd=seed)
    return bare


def seed_main(bare, tmp_path, files):
    """Add files to main directly, standing in for an earlier run of another type."""
    clone = tmp_path / f"seed-{uuid.uuid4().hex[:6]}"
    git("clone", "--quiet", str(bare), str(clone), cwd=tmp_path)
    for name, content in files.items():
        path = clone / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    git("add", "-A", cwd=clone)
    git(
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.com",
        "commit",
        "--quiet",
        "-m",
        "seed",
        cwd=clone,
    )
    git("push", "--quiet", "origin", "HEAD:main", cwd=clone)


def tree(bare, ref):
    return sorted(git("ls-tree", "-r", "--name-only", ref, cwd=bare).split())


def merge_into_main(bare, branch):
    """What the pull request does in production: main becomes the branch."""
    git("update-ref", "refs/heads/main", f"refs/heads/{branch}", cwd=bare)


def make_session(
    bare, tmp_path, monkeypatch, object_type="Councillors", mode=StorageMode.REPLACE
):
    session = _GitHubSession(
        organization="LGSF-Data",
        github_token="not-used",
        council_code="ABC",
        scraper_object_type=object_type,
        storage_mode=mode,
    )
    session.remote_url = str(bare)
    session.local_repo_path = str(tmp_path / "clones" / uuid.uuid4().hex[:8])
    monkeypatch.setattr(session, "_repo_exists_on_github", lambda: True)
    monkeypatch.setattr(session, "_check_if_repo_is_empty", lambda: False)
    return session


def run(bare, tmp_path, monkeypatch, files, **kwargs):
    """Stage files, commit and push a branch, then merge it as production would."""
    session = make_session(bare, tmp_path, monkeypatch, **kwargs)
    for name, content in files.items():
        session.write(Path(name), content)
    result = session._commit_files("Updated ABC")
    if not result.get("skipped"):
        merge_into_main(bare, result["branch"])
    session._cleanup_local_repo()
    return result


def test_files_go_under_the_types_directory(bare_repo, tmp_path, monkeypatch):
    run(bare_repo, tmp_path, monkeypatch, {"json/a.json": "{}", "raw/a.html": "<p>"})

    assert tree(bare_repo, "main") == [
        "Councillors/json/a.json",
        "Councillors/raw/a.html",
        "README.md",
    ]


def test_replace_run_removes_councillors_no_longer_present(
    bare_repo, tmp_path, monkeypatch
):
    run(
        bare_repo,
        tmp_path,
        monkeypatch,
        {"json/departed.json": "{}", "raw/departed.html": ""},
    )
    run(
        bare_repo,
        tmp_path,
        monkeypatch,
        {"json/current.json": "{}", "raw/current.html": ""},
    )

    assert tree(bare_repo, "main") == [
        "Councillors/json/current.json",
        "Councillors/raw/current.html",
        "README.md",
    ]


def test_replace_run_leaves_other_types_alone(bare_repo, tmp_path, monkeypatch):
    seed_main(
        bare_repo,
        tmp_path,
        {"Decisions/json/2026-01-01.json": "{}", "Decisions/_index.json": "[]"},
    )

    run(bare_repo, tmp_path, monkeypatch, {"json/a.json": "{}"})

    assert tree(bare_repo, "main") == [
        "Councillors/json/a.json",
        "Decisions/_index.json",
        "Decisions/json/2026-01-01.json",
        "README.md",
    ]


def test_accumulate_run_never_deletes(bare_repo, tmp_path, monkeypatch):
    kwargs = {"object_type": "Minutes", "mode": StorageMode.ACCUMULATE}
    run(bare_repo, tmp_path, monkeypatch, {"json/old-meeting.json": "{}"}, **kwargs)
    run(bare_repo, tmp_path, monkeypatch, {"json/new-meeting.json": "{}"}, **kwargs)

    assert tree(bare_repo, "main") == [
        "Minutes/json/new-meeting.json",
        "Minutes/json/old-meeting.json",
        "README.md",
    ]


def test_run_with_no_changes_makes_no_commit(bare_repo, tmp_path, monkeypatch):
    run(bare_repo, tmp_path, monkeypatch, {"json/a.json": "{}"})
    before = git("rev-parse", "main", cwd=bare_repo)

    result = run(bare_repo, tmp_path, monkeypatch, {"json/a.json": "{}"})

    assert result["skipped"]
    assert git("rev-parse", "main", cwd=bare_repo) == before


def test_open_reads_a_file_from_an_earlier_run(bare_repo, tmp_path, monkeypatch):
    run(bare_repo, tmp_path, monkeypatch, {"_last-run": "2026-10-23"})

    session = make_session(bare_repo, tmp_path, monkeypatch)
    session._ensure_local_repo()
    assert session.open(Path("_last-run")) == "2026-10-23"
    with pytest.raises(FileNotFoundError):
        session.open(Path("README.md"))  # the root isn't this type's directory
    session._cleanup_local_repo()
