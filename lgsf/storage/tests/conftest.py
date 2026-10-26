"""
Fixtures for the tests that run real git against bare repositories here.
"""

import subprocess

import pytest


@pytest.fixture
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
