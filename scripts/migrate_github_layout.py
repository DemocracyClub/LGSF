"""
Rewrite each LGSF-Data repository so every commit keeps councillor files
under ``Councillors/``.

Each data type now has its own directory in a council's repository,
``<Type>/json/`` and ``<Type>/raw/``, the same layout as
``data/<COUNCIL>/<Type>/`` locally, so that a run of one type can't add to
or delete another's files and a reader can tell them apart by path.

reps-wiki will read these repositories as a log, processing every commit on
``main`` in order and expecting each file at the same path throughout. A
commit that merely moved the files would leave the earlier commits in the
old layout, so instead this rewrites history: every commit is re-made with
its files where the current layout puts them, and ``main`` is force-pushed.
Commit authors, dates, messages, trees and merges are otherwise unchanged,
but every commit id changes. Content is never altered: the set of blobs at
the tip of ``main`` is checked to be the same before and after.

Three layouts appear in the repositories' history, never two in the same
commit, and the rewrite maps all of them to the current one:

- ``json/`` and ``raw/`` at the root, written from October 2025 until this
  change, become ``Councillors/json/`` and ``Councillors/raw/``.
- ``Councillors/json/`` and ``Councillors/raw/``, written from 2022 to
  October 2025, are already right and are left alone.
- ``Councillors/<name>.json`` with no ``json/`` level, written in 2021,
  becomes ``Councillors/json/<name>.json``.

Councillors are the only type the Lambda has ever written to GitHub, so a
root ``json/`` is always councillor data.

Run from the repo root, as a GitHub user who can push to the organisation's
repositories. The token comes from ``GITHUB_TOKEN``, or from ``gh auth
token`` when that isn't set. ``git filter-repo`` is a dev dependency, so
``uv run`` finds it:

    uv run python -m scripts.migrate_github_layout \\
        (--council ABC,DEF | --all-councils) [--dry-run] \\
        [--delete-other-branches] [--workers N] \\
        [--report github-layout-migration.csv]

For each repository it:

1. Clones it in full, every branch.
2. Looks through the history. If no commit has a file in an old place
   there is nothing to do. If a commit has both a root ``json/`` and a
   ``Councillors/`` directory, one of them is a stale copy of the other and
   nothing in the tree says which, so the repository is reported as failed
   and left alone for someone to look at. That happens when a scrape by the
   new backend lands before the rewrite, or one by the old backend lands
   after it, so run this between two daily scrapes and deploy before the
   next one.
3. Rewrites every ref with ``git filter-repo``, renaming paths as above.
4. Checks the result: no commit on any branch has a file in an old place,
   and the tip of ``main`` holds exactly the blobs it held before.
5. Force-pushes ``main``, with a lease on the commit it cloned, so a scrape
   that landed in the meantime makes the push fail rather than be lost.
   With ``--delete-other-branches`` every other branch on the remote is
   then deleted: leftover run branches whose pull request never merged,
   ``temp_merge_*`` branches, and backups from an earlier rewrite. Without
   it they are force-pushed in their rewritten form.

``--dry-run`` does everything but push: it reports what each repository
needs and whether its rewrite passed the checks. The report CSV lists every
repository with the outcome, commit counts before and after, and the old
and new ``main``, so a partial run can be finished by running again: a
repository already in the new layout is reported as such and left alone.

Run this after a daily scrape has finished, deploy the backend change
before the next one starts, and don't run it while a scrape is running.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Callable, Iterable

import requests
from rich.console import Console
from rich.table import Table

ORGANIZATION = "LGSF-Data"
DEFAULT_BRANCH = "main"
TYPE_DIR = "Councillors"

#: A path in one of the old layouts. The first alternative is the root
#: ``json/`` and ``raw/``; the second the 2021 flat ``Councillors/x.json``.
OLD_PATH = re.compile(rb"^(?:(?:json|raw)/|Councillors/[^/]+\.json$)")

#: The body of git filter-repo's --filename-callback, which it wraps in a
#: function taking ``filename`` as bytes. It must agree with OLD_PATH.
RENAME_CALLBACK = """
if filename.startswith(b"json/") or filename.startswith(b"raw/"):
    return b"Councillors/" + filename
if (
    filename.startswith(b"Councillors/")
    and filename.count(b"/") == 1
    and filename.endswith(b".json")
):
    return b"Councillors/json/" + filename[len(b"Councillors/"):]
return filename
"""


@dataclass
class Result:
    repo: str
    #: rewritten | already-migrated | empty | failed
    action: str
    commits_before: int = 0
    commits_after: int = 0
    old_main: str = ""
    new_main: str = ""
    branches_deleted: int = 0
    error: str = ""


def git(args: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=check,
        capture_output=True,
        text=False,
        timeout=1800,
    )


def out(args: list[str], cwd: Path) -> bytes:
    return git(args, cwd).stdout


def old_paths(clone: Path, refs: list[str]) -> set[bytes]:
    """Paths in an old layout touched by any commit reachable from refs."""
    names = out(["log", *refs, "--format=", "--name-only", "--diff-filter=AMRC"], clone)
    return {line for line in names.split(b"\n") if OLD_PATH.match(line)}


def ambiguous_commits(clone: Path) -> list[bytes]:
    """Commits holding both a root json/ and Councillors/: the rename would collide."""
    found = []
    for commit in out(["rev-list", "--all"], clone).split():
        top = set(out(["ls-tree", "--name-only", commit], clone).split(b"\n"))
        if b"json" in top and TYPE_DIR.encode() in top:
            found.append(commit)
    return found


def blobs_at(clone: Path, ref: str) -> set[bytes]:
    """The blob ids at a commit, with the file names stripped: content only."""
    return {line.split()[2] for line in out(["ls-tree", "-r", ref], clone).splitlines()}


def count_commits(clone: Path, ref: str) -> int:
    return int(out(["rev-list", "--count", ref], clone))


def commits_per_branch(clone: Path) -> dict[str, int]:
    """
    Commit count reachable from each branch, by name. Before the rewrite the
    branches are remote-tracking refs, afterwards local ones, so both are
    read under their bare names.
    """
    counts: dict[str, int] = {}
    refs = out(
        ["for-each-ref", "--format=%(refname)", "refs/heads/", "refs/remotes/origin/"],
        clone,
    )
    for ref in refs.decode().split():
        name = ref.removeprefix("refs/remotes/origin/").removeprefix("refs/heads/")
        if name == "HEAD":
            continue
        counts[name] = count_commits(clone, ref)
    return counts


def migrate_repo(
    repo: str,
    remote_url: str,
    workdir: Path,
    dry_run: bool,
    delete_other_branches: bool = False,
) -> Result:
    """Bring one repository's whole history to the per-type layout. Never raises for a repo's own failure."""
    workdir.mkdir(parents=True, exist_ok=True)
    clone = workdir / repo
    shutil.rmtree(clone, ignore_errors=True)
    try:
        return _migrate_clone(repo, remote_url, clone, dry_run, delete_other_branches)
    except subprocess.CalledProcessError as e:
        detail = (e.stderr or e.stdout or b"").decode("utf-8", "replace").strip()
        return Result(repo, "failed", error=f"{e.cmd[:3]}: {detail}"[:1000])
    except Exception as e:  # keep going with the other repositories
        return Result(repo, "failed", error=f"{type(e).__name__}: {e}")
    finally:
        shutil.rmtree(clone, ignore_errors=True)


def _migrate_clone(
    repo: str, remote_url: str, clone: Path, dry_run: bool, delete_other_branches: bool
) -> Result:
    cloned = git(
        ["clone", "--quiet", "--branch", DEFAULT_BRANCH, remote_url, str(clone)],
        cwd=clone.parent,
        check=False,
    )
    if cloned.returncode != 0:
        stderr = cloned.stderr.decode("utf-8", "replace")
        no_main = (
            "Could not find remote branch" in stderr
            or "does not have any commits yet" in stderr
            or f"Remote branch {DEFAULT_BRANCH} not found" in stderr
        )
        if no_main:
            return Result(repo, "empty")
        return Result(repo, "failed", error=stderr.strip())

    result = Result(repo, "rewritten")
    result.old_main = out(["rev-parse", DEFAULT_BRANCH], clone).decode().strip()
    result.commits_before = count_commits(clone, "--all")

    if not old_paths(clone, ["--all"]):
        result.action = "already-migrated"
        result.commits_after = result.commits_before
        result.new_main = result.old_main
        if delete_other_branches and not dry_run:
            result.branches_deleted = delete_remote_branches(clone)
        return result

    if clashes := ambiguous_commits(clone):
        result.action = "failed"
        result.error = (
            f"{len(clashes)} commit(s) hold both a root json/ and {TYPE_DIR}/, "
            f"e.g. {clashes[0].decode()[:10]}; the rename would be ambiguous"
        )
        return result

    blobs_before = blobs_at(clone, DEFAULT_BRANCH)
    counts_before = commits_per_branch(clone)

    # filter-repo rewrites every ref, turns the remote-tracking branches into
    # local ones and removes the origin remote so nothing is pushed by accident.
    # A commit that only moved files between layouts becomes empty once both
    # sides are renamed; keep it (and its merge) so that readers walking the
    # commits see the same sequence of snapshots as before.
    git(
        [
            "filter-repo",
            "--quiet",
            "--prune-empty=never",
            "--prune-degenerate=never",
            "--filename-callback",
            RENAME_CALLBACK,
        ],
        cwd=clone,
    )

    result.new_main = out(["rev-parse", DEFAULT_BRANCH], clone).decode().strip()
    result.commits_after = count_commits(clone, "--all")

    if (counts_after := commits_per_branch(clone)) != counts_before:
        changed = sorted(
            b for b in counts_before if counts_after.get(b) != counts_before[b]
        )
        result.action = "failed"
        result.error = f"commit count changed on {', '.join(changed)}"
        return result
    if leftover := old_paths(clone, ["--all"]):
        result.action = "failed"
        result.error = f"after rewriting, {len(leftover)} path(s) are still in an old place, e.g. {sorted(leftover)[0].decode()}"
        return result
    if blobs_at(clone, DEFAULT_BRANCH) != blobs_before:
        result.action = "failed"
        result.error = f"the content at the tip of {DEFAULT_BRANCH} changed"
        return result

    if dry_run:
        return result

    git(["remote", "add", "origin", remote_url], cwd=clone)
    git(
        [
            "push",
            "--quiet",
            f"--force-with-lease=refs/heads/{DEFAULT_BRANCH}:{result.old_main}",
            "origin",
            DEFAULT_BRANCH,
        ],
        cwd=clone,
    )

    if delete_other_branches:
        result.branches_deleted = delete_remote_branches(clone)
    else:
        others = other_branches(clone, "refs/heads/")
        if others:
            git(["push", "--quiet", "--force", "origin", *others], cwd=clone)
    return result


def other_branches(clone: Path, prefix: str) -> list[str]:
    """Branch names under prefix other than the default branch."""
    names = out(["for-each-ref", "--format=%(refname)", prefix], clone).decode().split()
    return [
        name.removeprefix(prefix)
        for name in names
        if name.removeprefix(prefix) not in (DEFAULT_BRANCH, "HEAD")
    ]


def delete_remote_branches(clone: Path) -> int:
    """
    Delete every branch but the default one on origin. After filter-repo the
    branches are local refs; in an untouched clone they are remote-tracking.
    """
    others = sorted(
        set(other_branches(clone, "refs/heads/"))
        | set(other_branches(clone, "refs/remotes/origin/"))
    )
    if others:
        git(["push", "--quiet", "origin", "--delete", *others], cwd=clone)
    return len(others)


def run(
    repos: Iterable[str],
    remote_for: Callable[[str], str],
    workdir: Path,
    dry_run: bool,
    workers: int = 4,
    delete_other_branches: bool = False,
) -> list[Result]:
    workdir.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(
            pool.map(
                lambda repo: migrate_repo(
                    repo, remote_for(repo), workdir, dry_run, delete_other_branches
                ),
                repos,
            )
        )


def write_report(results: list[Result], path: Path) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[x.name for x in fields(Result)])
        writer.writeheader()
        for result in results:
            writer.writerow(asdict(result))


def print_summary(results: list[Result], console: Console, dry_run: bool) -> None:
    table = Table(
        title="Repository layout migration" + (" (dry run)" if dry_run else "")
    )
    for column in (
        "Repo",
        "Action",
        "Commits",
        "Old main",
        "New main",
        "Deleted",
        "Error",
    ):
        table.add_column(column)
    for r in results:
        table.add_row(
            r.repo,
            r.action,
            f"{r.commits_before} -> {r.commits_after}",
            r.old_main[:10],
            r.new_main[:10],
            str(r.branches_deleted),
            r.error[:80],
        )
    console.print(table)
    counts: dict[str, int] = {}
    for r in results:
        counts[r.action] = counts.get(r.action, 0) + 1
    console.print(", ".join(f"{action}: {n}" for action, n in sorted(counts.items())))


# --- GitHub ---------------------------------------------------------------


def github_token() -> str:
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return token
    try:
        return subprocess.run(
            ["gh", "auth", "token"], check=True, capture_output=True, text=True
        ).stdout.strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        sys.exit("Set GITHUB_TOKEN, or log in with `gh auth login`.")


def list_repos(organization: str, token: str) -> list[str]:
    session = requests.Session()
    session.headers.update(
        {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    )
    names: list[str] = []
    url: str | None = (
        f"https://api.github.com/orgs/{organization}/repos?per_page=100&type=all"
    )
    while url:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        names.extend(repo["name"] for repo in response.json())
        url = response.links.get("next", {}).get("url")
    return sorted(names)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument(
        "--council", help="Comma-separated council codes (repository names)"
    )
    which.add_argument(
        "--all-councils",
        action="store_true",
        help="Every repository in the organisation",
    )
    parser.add_argument("--organization", default=ORGANIZATION)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Clone, rewrite and check locally; push nothing",
    )
    parser.add_argument(
        "--delete-other-branches",
        action="store_true",
        help="After pushing main, delete every other branch on the remote",
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--report", type=Path, default=Path("github-layout-migration.csv")
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=Path("/tmp/lgsf-layout-migration"),
        help="Where repositories are cloned while being worked on",
    )
    args = parser.parse_args(argv)

    console = Console()
    token = github_token()
    if args.all_councils:
        repos = list_repos(args.organization, token)
    else:
        repos = [
            code.strip().upper() for code in args.council.split(",") if code.strip()
        ]
    console.print(f"{len(repos)} repositories in {args.organization}")

    def remote_for(repo: str) -> str:
        return (
            f"https://x-access-token:{token}@github.com/{args.organization}/{repo}.git"
        )

    results = run(
        repos,
        remote_for,
        args.workdir,
        args.dry_run,
        args.workers,
        args.delete_other_branches,
    )
    write_report(results, args.report)
    print_summary(results, console, args.dry_run)
    console.print(f"Report written to {args.report}")
    return 1 if any(r.action == "failed" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
