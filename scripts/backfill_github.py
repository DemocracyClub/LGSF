"""
Seed the LGSF-Data repositories with minutes and decisions already on S3.

Laptop backfills wrote each council's minutes and decisions to S3. The
daily AWS run writes them to the council's GitHub repository instead,
under ``Minutes/`` and ``Decisions/``. Without this, a repository's first
Lambda run of a type starts from nothing: no records, and no
``_index.json``, the file that records what has been seen and each page's
ETag, so the run re-fetches its whole window.

For each council and type this copies the records from
``s3://<bucket>/<prefix>/<COUNCIL>/<Type>/`` into the repository's
``<Type>/``, as one commit, and pushes it:

- ``json/``, ``raw/`` and the bookkeeping files (``_index.json``,
  ``_last-run``) are copied byte for byte.
- ``documents/`` is not: the records name their documents by content key,
  and the files themselves stay in S3, where the worker reads and writes
  them (``<prefix>/<COUNCIL>/documents/``).
- Local run artefacts (``runlog.json``, ``scrape_summary.json``) are left
  out; the Lambda never writes them.

These are accumulate types, so a file already in the repository and not on
S3 stays. A file on both is overwritten with the S3 copy. A council and
type whose files are all already there makes no commit.

Each commit is authored as LGSF Bot and made with your own git
configuration, so it is signed as your commits are. Its message carries
the trailers every scraper commit does, ``LGSF-Scraper-Type``, plus
``LGSF-Backfill`` naming where on S3 it came from. A push is fast-forward
only; if the daily run pushed to the repository meanwhile, the council is
redone on a fresh clone, up to three times.

Run from the repo root, with AWS credentials for the bucket and a GitHub
token that can push to the organisation (``GITHUB_TOKEN``, or ``gh auth
token``). Run it after the documents migration, which rewrites the
decision records to name content keys, and before a type joins the daily
run:

    LGSF_S3_BUCKET=lgsf-data-archive uv run python -m scripts.backfill_github \\
        --types minutes[,decisions] (--council KIR,STO | --all-councils) \\
        [--prefix merged-data] [--dry-run] [--workers N] [--report backfill.csv]

``--dry-run`` lists what each council has on S3 and whether its repository
exists, and downloads, commits and pushes nothing. A council with no
repository is skipped and reported: the repository is made by the
council's first councillors run.

Records pass through this machine on their way from S3 to GitHub. That is
the records only, a few tens of megabytes for the largest council;
documents, which run to gigabytes, are not touched.
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Callable, Iterable

from rich.console import Console
from rich.table import Table

from lgsf.scraper_types import SCRAPER_TYPES
from lgsf.storage import s3
from scripts.migrate_github_layout import github_token

ORGANIZATION = "LGSF-Data"
DEFAULT_BRANCH = "main"
DEFAULT_PREFIX = "merged-data"

#: Files under a type's directory on S3 that aren't records: local run
#: artefacts the Lambda never writes.
SKIP_NAMES = frozenset({"runlog.json", "scrape_summary.json"})

#: Types whose runs replace everything: a backfill of one would be a
#: snapshot of some earlier day, which the next run would undo.
REPLACE_TYPES = frozenset({"councillors"})

BOT = {
    "GIT_AUTHOR_NAME": "LGSF Bot",
    "GIT_AUTHOR_EMAIL": "lgsf-bot@democracyclub.org.uk",
    "GIT_COMMITTER_NAME": "LGSF Bot",
    "GIT_COMMITTER_EMAIL": "lgsf-bot@democracyclub.org.uk",
}

PUSH_ATTEMPTS = 3


def object_type(scraper_type: str) -> str:
    """The directory a type's records live in: minutes -> Minutes."""
    return scraper_type.capitalize()


def is_record(relative_key: str) -> bool:
    """Whether a key under <COUNCIL>/<Type>/ is something to copy."""
    if relative_key.startswith("documents/"):
        return False
    name = relative_key.rsplit("/", 1)[-1]
    return bool(name) and name not in SKIP_NAMES and not name.startswith(".tmp-")


@dataclass
class Result:
    council: str
    #: committed | unchanged | nothing-on-s3 | no-repository | would-commit | failed
    action: str
    types: str = ""
    files: int = 0
    bytes: int = 0
    commits: int = 0
    head: str = ""
    error: str = ""


def git(args: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    # Your own git configuration, so the commits are signed as yours are;
    # only the identity is the bot's, as on every other scraper commit.
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        env={**os.environ, **BOT},
        check=check,
        capture_output=True,
        text=True,
        timeout=1800,
    )


class Backfill:
    def __init__(
        self,
        client,
        bucket: str,
        prefix: str,
        scraper_types: list[str],
        remote_for: Callable[[str], str],
        workdir: Path,
        dry_run: bool = False,
        workers: int = 8,
        out: Callable[[str], None] = print,
    ):
        unknown = [t for t in scraper_types if t not in SCRAPER_TYPES]
        if unknown:
            raise ValueError(f"Unknown scraper type(s): {', '.join(unknown)}")
        replacing = [t for t in scraper_types if t in REPLACE_TYPES]
        if replacing:
            raise ValueError(
                f"{', '.join(replacing)} replace everything each run; "
                "backfilling an old snapshot would only be undone"
            )
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        # Registry order, so a council's commits come in run order
        self.scraper_types = [t for t in SCRAPER_TYPES if t in scraper_types]
        self.remote_for = remote_for
        self.workdir = Path(workdir)
        self.dry_run = dry_run
        self.workers = workers
        self.out = out

    # ---- S3 ----

    def councils(self) -> list[str]:
        """Every council with a directory under the prefix."""
        prefix = f"{self.prefix}/" if self.prefix else ""
        found = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self.bucket, Prefix=prefix, Delimiter="/"
        ):
            for common in page.get("CommonPrefixes", []):
                found.append(common["Prefix"][len(prefix) :].strip("/"))
        return sorted(found)

    def records(self, council: str, scraper_type: str) -> dict[str, int]:
        """{key relative to <Type>/: size} for the records on S3."""
        root = s3.join_key(self.prefix, council, object_type(scraper_type))
        found = {}
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f"{root}/"):
            for item in page.get("Contents", []):
                relative = item["Key"][len(root) + 1 :]
                if is_record(relative):
                    found[relative] = item["Size"]
        return found

    def download(
        self, council: str, scraper_type: str, keys: Iterable[str], into: Path
    ):
        root = s3.join_key(self.prefix, council, object_type(scraper_type))

        def fetch(relative):
            body = self.client.get_object(Bucket=self.bucket, Key=f"{root}/{relative}")[
                "Body"
            ].read()
            path = into / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)

        with ThreadPoolExecutor(self.workers) as pool:
            list(pool.map(fetch, keys))

    # ---- one council ----

    def repository_exists(self, council: str) -> bool:
        return (
            git(
                ["ls-remote", "--exit-code", self.remote_for(council), DEFAULT_BRANCH],
                cwd=self.workdir,
                check=False,
            ).returncode
            == 0
        )

    def run_council(self, council: str) -> Result:
        """Never raises for the council's own failure."""
        self.workdir.mkdir(parents=True, exist_ok=True)
        try:
            return self._run_council(council)
        except subprocess.CalledProcessError as e:
            detail = (e.stderr or e.stdout or "").strip()
            return Result(council, "failed", error=f"{e.cmd[1]}: {detail}"[:1000])
        except Exception as e:
            return Result(council, "failed", error=f"{type(e).__name__}: {e}"[:1000])
        finally:
            shutil.rmtree(self.workdir / council, ignore_errors=True)

    def _run_council(self, council: str) -> Result:
        on_s3 = {t: self.records(council, t) for t in self.scraper_types}
        on_s3 = {t: keys for t, keys in on_s3.items() if keys}
        result = Result(
            council,
            "nothing-on-s3",
            types=",".join(on_s3),
            files=sum(len(k) for k in on_s3.values()),
            bytes=sum(sum(k.values()) for k in on_s3.values()),
        )
        if not on_s3:
            return result
        if not self.repository_exists(council):
            result.action = "no-repository"
            return result
        if self.dry_run:
            result.action = "would-commit"
            return result

        # Download once; a retry after a rejected push reuses the files.
        staging = self.workdir / council / "s3"
        for scraper_type, keys in on_s3.items():
            self.download(
                council, scraper_type, keys, staging / object_type(scraper_type)
            )

        for attempt in range(1, PUSH_ATTEMPTS + 1):
            pushed, commits, head = self._commit_and_push(council, on_s3, staging)
            if pushed:
                result.commits = commits
                result.head = head
                result.action = "committed" if commits else "unchanged"
                return result
            self.out(f"{council}: main moved during the backfill, redoing ({attempt})")
        raise RuntimeError(f"main kept moving; gave up after {PUSH_ATTEMPTS} attempts")

    def _commit_and_push(self, council, on_s3, staging) -> tuple[bool, int, str]:
        clone = self.workdir / council / "repo"
        shutil.rmtree(clone, ignore_errors=True)
        git(
            [
                "clone",
                "--quiet",
                "--depth",
                "1",
                "--branch",
                DEFAULT_BRANCH,
                self.remote_for(council),
                str(clone),
            ],
            cwd=self.workdir,
        )
        commits = 0
        for scraper_type in on_s3:
            directory = object_type(scraper_type)
            shutil.copytree(staging / directory, clone / directory, dirs_exist_ok=True)
            git(["add", "--", directory], cwd=clone)
            if (
                git(["diff", "--cached", "--quiet"], cwd=clone, check=False).returncode
                == 0
            ):
                continue
            git(
                ["commit", "--quiet", "-m", self.message(council, scraper_type)],
                cwd=clone,
            )
            commits += 1
        head = git(["rev-parse", "HEAD"], cwd=clone).stdout.strip()
        if not commits:
            return True, 0, head
        pushed = git(
            ["push", "--quiet", "origin", DEFAULT_BRANCH], cwd=clone, check=False
        )
        if pushed.returncode == 0:
            return True, commits, head
        if "rejected" in pushed.stderr or "fetch first" in pushed.stderr:
            return False, 0, ""
        raise subprocess.CalledProcessError(
            pushed.returncode, pushed.args, pushed.stdout, pushed.stderr
        )

    def message(self, council: str, scraper_type: str) -> str:
        directory = object_type(scraper_type)
        source = f"s3://{s3.join_key(self.bucket, self.prefix, council, directory)}/"
        return (
            f"Backfill {council} {scraper_type} from S3\n"
            "\n"
            f"The {scraper_type} a laptop backfill stored on S3, so the daily\n"
            "run carries on from them rather than starting from nothing. The\n"
            "documents they name are in S3 under the same prefix.\n"
            "\n"
            f"LGSF-Scraper-Type: {directory}\n"
            f"LGSF-Backfill: {source}\n"
        )

    def run(self, councils: Iterable[str], workers: int = 4) -> list[Result]:
        with ThreadPoolExecutor(workers) as pool:
            return list(pool.map(self.run_council, councils))


# ---- reporting ----


def write_report(results: list[Result], path: Path) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[x.name for x in fields(Result)])
        writer.writeheader()
        for result in results:
            writer.writerow(asdict(result))


def print_summary(results: list[Result], console: Console, dry_run: bool) -> None:
    table = Table(title="Backfill to GitHub" + (" (dry run)" if dry_run else ""))
    for column in ("Council", "Action", "Types", "Files", "MB", "Commits", "Error"):
        table.add_column(column)
    for r in results:
        table.add_row(
            r.council,
            r.action,
            r.types,
            f"{r.files:,}",
            f"{r.bytes / 1e6:,.1f}",
            str(r.commits),
            r.error[:80],
        )
    console.print(table)
    counts: dict[str, int] = {}
    for r in results:
        counts[r.action] = counts.get(r.action, 0) + 1
    console.print(", ".join(f"{a}: {n}" for a, n in sorted(counts.items())))
    total = sum(r.bytes for r in results)
    console.print(
        f"{sum(r.files for r in results):,} files, {total / 1e9:,.2f} GB of records"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--types",
        required=True,
        help="Comma-separated scraper types to backfill, e.g. minutes,decisions",
    )
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--council", help="Comma-separated council codes")
    which.add_argument(
        "--all-councils", action="store_true", help="Every council under the prefix"
    )
    parser.add_argument(
        "--prefix", default=DEFAULT_PREFIX, help=f"S3 prefix (default {DEFAULT_PREFIX})"
    )
    parser.add_argument("--organization", default=ORGANIZATION)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List and check; download, commit and push nothing",
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Councils at once (default 4)"
    )
    parser.add_argument("--report", type=Path, default=Path("github-backfill.csv"))
    parser.add_argument(
        "--workdir", type=Path, default=Path("/tmp/lgsf-github-backfill")
    )
    args = parser.parse_args(argv)

    console = Console()
    token = github_token()

    def remote_for(council: str) -> str:
        return f"https://x-access-token:{token}@github.com/{args.organization}/{council}.git"

    backfill = Backfill(
        s3.get_client(),
        s3.bucket_from_environment(),
        args.prefix,
        [t.strip() for t in args.types.split(",") if t.strip()],
        remote_for,
        args.workdir,
        dry_run=args.dry_run,
        out=console.print,
    )
    if args.all_councils:
        councils = backfill.councils()
    else:
        councils = [
            s3.safe_name(c.strip().upper(), "council")
            for c in args.council.split(",")
            if c.strip()
        ]
    console.print(
        f"Backfilling {', '.join(backfill.scraper_types)} for {len(councils)} councils "
        f"from s3://{backfill.bucket}/{backfill.prefix}"
    )

    results = backfill.run(councils, workers=args.workers)
    write_report(results, args.report)
    print_summary(results, console, args.dry_run)
    console.print(f"Report written to {args.report}")
    return 1 if any(r.action == "failed" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
