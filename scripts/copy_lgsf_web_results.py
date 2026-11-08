"""
Copy lgsf-web's Docling conversions into the document pipeline's layout.

Before the pipeline ran in LGSF, lgsf-web converted the decision documents
of about two dozen councils with Docling 2.130.0 and wrote its results
beside each document in the old layout::

    <bucket>/data/<COUNCIL>/Decisions/documents/<old key>.status.json
                                                 <old key>.docling.json
                                                 <old key>.md
                                                 <old key>.chunks.jsonl
                                                 <old key>.titan-v2.jsonl

The old key is the one the decision gave the file, such as
``2026-01-12-3064-s82290.pdf``. The documents migration copied the files
to content keys under ``merged-data/<COUNCIL>/documents/<sha256><ext>``,
where the pipeline looks for them (see docs/document-pipeline.md).

Converting takes a second a page, so the Docling output is worth keeping.
The chunks and embeddings aren't: lgsf-web's chunks carry decision headers
the pipeline's don't, so chunking and embedding run again. For each old
status, by what lgsf-web's extraction found:

- ``ok`` or ``partial``: the ``.docling.json`` and ``.md`` are copied
  inside S3 to ``<new key>.docling-v2.json`` and ``<new key>.md``, and a
  status in the pipeline's format is written with the outcome ``ok`` and
  the extract stage alone, so the pipeline chunks and embeds the file
  next without converting it again.
- ``restricted``, ``empty``, ``not_document`` or ``unsupported``: only a
  status is written, with the matching outcome, so dispatch never
  downloads the file.
- ``failed`` or ``timeout``: nothing is written. The pipeline tries the
  file afresh.

The new key is found by the content hash in the old status, from one
listing of the council's documents. A file with no key there (a document
the migration left behind) is counted and skipped. A file that already
has a status in the new layout is left alone, so the script can be run
again. The status is written last, so a run that's stopped never leaves a
status vouching for files that aren't there. Nothing in the old layout is
changed or deleted.

Run from the repo root, with the bucket and AWS credentials set as for a
scrape:

    LGSF_S3_BUCKET=lgsf-data-archive uv run python -m scripts.copy_lgsf_web_results \\
        [--council PEM,FLN] [--dry-run] [--workers N]

``--dry-run`` lists what each file would get and changes nothing. Without
``--council``, every council with results in the old layout is done.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable

from botocore.exceptions import ClientError

from lgsf.embeddings.extract import EXTRACTED, FAILURES, LEFT_OUT, ExtractStatus
from lgsf.embeddings.formats import DocumentFormat
from lgsf.embeddings.sidecars import (
    OUTCOMES,
    ExtractEntry,
    FileStatus,
    Suffix,
    result_key,
    write_status,
)
from lgsf.embeddings.stores import S3Store
from lgsf.storage import s3
from lgsf.storage.documents.base import is_content_key
from lgsf.storage.s3 import DOCUMENTS_DIR

OLD_PREFIX = "data"
NEW_PREFIX = "merged-data"

#: The old result files, by suffix, and the pipeline's suffix for each
#: one that's copied. Chunks and embeddings aren't.
OLD_STATUS = "status.json"
COPIED = {"docling.json": str(Suffix.DOCLING), "md": str(Suffix.TEXT)}


@dataclass
class Action:
    """What one old status gets: decided first, then carried out."""

    old_key: str
    #: copy | cheap | present | not_in_merged | skipped_failed | problem
    kind: str
    new_key: str | None = None
    status: FileStatus | None = None
    note: str = ""

    def line(self, council: str) -> str:
        target = f" -> {self.new_key}" if self.new_key else ""
        note = f" ({self.note})" if self.note else ""
        return f"{council}: {self.old_key}{target}: {self.kind}{note}"


@dataclass
class Report:
    council: str
    statuses: int = 0
    copied: int = 0
    cheap: int = 0
    present: int = 0
    not_in_merged: int = 0
    skipped_failed: int = 0
    #: Old statuses that couldn't be read or lack the files to copy.
    problems: list[str] = field(default_factory=list)
    #: Files the store failed on. Nothing was written for them.
    errors: list[str] = field(default_factory=list)

    def count(self, action: Action):
        if action.kind == "copy":
            self.copied += 1
        elif action.kind == "cheap":
            self.cheap += 1
        elif action.kind == "present":
            self.present += 1
        elif action.kind == "not_in_merged":
            self.not_in_merged += 1
        elif action.kind == "skipped_failed":
            self.skipped_failed += 1
        else:
            self.problems.append(f"{action.old_key}: {action.note}")

    @property
    def failed(self) -> bool:
        return bool(self.errors)


class Copier:
    def __init__(
        self,
        client,
        bucket: str,
        old_prefix: str = OLD_PREFIX,
        new_prefix: str = NEW_PREFIX,
        dry_run: bool = False,
        workers: int = 16,
        out: Callable[[str], None] = print,
    ):
        # Full keys are passed to the store, as the two layouts live
        # under different prefixes.
        self.store = S3Store(bucket, "", client=client)
        self.bucket = bucket
        self.old_prefix = old_prefix.strip("/")
        self.new_prefix = new_prefix.strip("/")
        self.dry_run = dry_run
        self.workers = workers
        self.out = out

    # ---- keys ----

    def old_dir(self, council: str) -> str:
        return s3.join_key(self.old_prefix, council, "Decisions", DOCUMENTS_DIR) + "/"

    def new_dir(self, council: str) -> str:
        return s3.join_key(self.new_prefix, council, DOCUMENTS_DIR) + "/"

    def councils(self) -> list[str]:
        """Every council with results in the old layout."""
        root = f"{self.old_prefix}/" if self.old_prefix else ""
        found = []
        paginator = self.store.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=root, Delimiter="/"):
            for common in page.get("CommonPrefixes", []):
                council = common["Prefix"][len(root) :].strip("/")
                if any(
                    key.endswith(f".{OLD_STATUS}")
                    for key in self.store.list_prefix(self.old_dir(council))
                ):
                    found.append(council)
        return sorted(found)

    # ---- one council ----

    def run_council(self, council: str) -> Report:
        report = Report(council)
        old_dir, new_dir = self.old_dir(council), self.new_dir(council)
        old_keys = set(self.store.list_prefix(old_dir))
        new_keys = set(self.store.list_prefix(new_dir))
        status_keys = sorted(k for k in old_keys if k.endswith(f".{OLD_STATUS}"))
        report.statuses = len(status_keys)
        if not status_keys:
            return report

        # Content keys in the new layout, by their hash.
        by_digest = {}
        for key in new_keys:
            name = key.removeprefix(new_dir)
            if is_content_key(name):
                by_digest[name.split(".", 1)[0]] = name

        def read(key):
            try:
                return json.loads(self.store.read_bytes(key))
            except (ValueError, FileNotFoundError, ClientError) as exc:
                return exc

        with ThreadPoolExecutor(self.workers) as pool:
            old_statuses = list(pool.map(read, status_keys))

        # Decided in key order, so two old keys for the same bytes (a
        # document linked from two decisions) give one copy.
        actions = []
        decided: dict[str, str] = {}
        for key, data in zip(status_keys, old_statuses):
            old_key = key.removeprefix(old_dir).removesuffix(f".{OLD_STATUS}")
            action = self.decide(council, old_key, data, old_keys, by_digest, new_keys)
            if action.status is not None:
                new_keys.add(result_key(new_dir + action.new_key, Suffix.STATUS))
                decided[action.new_key] = old_key
            elif action.kind == "present" and action.new_key in decided:
                action.note = f"same bytes as {decided[action.new_key]}"
            actions.append(action)

        for action in actions:
            if self.dry_run:
                self.out(action.line(council))
            if self.dry_run or action.status is None:
                report.count(action)
        if self.dry_run:
            return report

        to_do = [a for a in actions if a.status is not None]
        lock = threading.Lock()

        def carry_out(action):
            try:
                self.write(council, action)
            except Exception as exc:  # noqa: BLE001
                with lock:
                    report.errors.append(
                        f"{action.old_key}: {type(exc).__name__}: {exc}"
                    )
                return
            with lock:
                report.count(action)

        with ThreadPoolExecutor(self.workers) as pool:
            list(pool.map(carry_out, to_do))
        return report

    def decide(
        self,
        council: str,
        old_key: str,
        data,
        old_keys: set[str],
        by_digest: dict[str, str],
        new_keys: set[str],
    ) -> Action:
        """What one old status gets, from what it says and what's there."""
        if isinstance(data, Exception):
            return Action(old_key, "problem", note=f"unreadable: {data}")
        content_hash = str((data or {}).get("content_hash") or "")
        digest = content_hash.removeprefix("sha256:")
        if not content_hash.startswith("sha256:") or len(digest) != 64:
            return Action(old_key, "problem", note=f"no content hash: {data!r:.80}")
        new_key = by_digest.get(digest)
        if new_key is None:
            return Action(old_key, "not_in_merged", note=digest)
        new_dir = self.new_dir(council)
        if result_key(new_dir + new_key, Suffix.STATUS) in new_keys:
            return Action(old_key, "present", new_key)

        extract = data.get("extract") or {}
        try:
            extract_status = ExtractStatus(extract.get("status"))
        except ValueError:
            return Action(
                old_key,
                "skipped_failed",
                new_key,
                note=f"extract status {extract.get('status')!r}",
            )
        if extract_status in FAILURES:
            return Action(old_key, "skipped_failed", new_key, note=extract_status)
        old_format = data.get("format")
        try:
            document_format = DocumentFormat(old_format) if old_format else None
        except ValueError:
            return Action(old_key, "problem", new_key, note=f"format {old_format!r}")

        status = FileStatus(
            council=council,
            storage_key=new_key,
            content_hash=content_hash,
            format=document_format,
            outcome=OUTCOMES[extract_status],
            error=extract.get("error"),
            extract=ExtractEntry(
                status=extract_status,
                tool_version=extract.get("docling_version"),
                finished_at=extract.get("finished_at"),
                pages=extract.get("pages"),
                seconds=extract.get("seconds"),
                device=extract.get("device"),
            ),
        )
        if extract_status in LEFT_OUT:
            return Action(old_key, "cheap", new_key, status, note=status.outcome)

        assert extract_status in EXTRACTED
        old_dir = self.old_dir(council)
        missing = [
            suffix
            for suffix in COPIED
            if result_key(old_dir + old_key, suffix) not in old_keys
        ]
        if missing:
            return Action(
                old_key, "problem", new_key, note=f"no .{' or .'.join(missing)} to copy"
            )
        pages = extract.get("pages")
        note = f"{extract_status}, {pages} pages" if pages else str(extract_status)
        return Action(old_key, "copy", new_key, status, note=note)

    def write(self, council: str, action: Action):
        """Copy the files, then write the status."""
        old_dir, new_dir = self.old_dir(council), self.new_dir(council)
        if action.kind == "copy":
            for old_suffix, new_suffix in COPIED.items():
                target = result_key(new_dir + action.new_key, new_suffix)
                self.store.client.copy_object(
                    Bucket=self.bucket,
                    Key=target,
                    CopySource={
                        "Bucket": self.bucket,
                        "Key": result_key(old_dir + action.old_key, old_suffix),
                    },
                    MetadataDirective="REPLACE",
                    ContentType=s3.content_type(target),
                )
        write_status(self.store, new_dir + action.new_key, action.status)


# ---- reporting ----


def summary(reports: list[Report], dry_run: bool) -> str:
    columns = (
        ("Council", "council"),
        ("Statuses", "statuses"),
        ("Copied", "copied"),
        ("Cheap", "cheap"),
        ("Present", "present"),
        ("Not in merged-data", "not_in_merged"),
        ("Skipped failed", "skipped_failed"),
        ("Problems", None),
        ("Errors", None),
    )
    rows = []
    for report in reports:
        rows.append(
            [report.council]
            + [f"{getattr(report, attr):,}" for _, attr in columns[1:-2]]
            + [f"{len(report.problems):,}", f"{len(report.errors):,}"]
        )
    totals = ["Total"] + [
        f"{sum(getattr(r, attr) for r in reports):,}" for _, attr in columns[1:-2]
    ]
    totals += [
        f"{sum(len(r.problems) for r in reports):,}",
        f"{sum(len(r.errors) for r in reports):,}",
    ]
    rows.append(totals)
    widths = [
        max(len(name), *(len(row[i]) for row in rows))
        for i, (name, _) in enumerate(columns)
    ]
    lines = ["  ".join(name.ljust(widths[i]) for i, (name, _) in enumerate(columns))]
    for row in rows:
        lines.append(
            "  ".join(
                (cell.ljust if i == 0 else cell.rjust)(widths[i])
                for i, cell in enumerate(row)
            )
        )
    title = "Would copy" if dry_run else "Copied"
    return f"{title} lgsf-web's results\n" + "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.copy_lgsf_web_results",
        description="Copy lgsf-web's Docling conversions into the pipeline's layout.",
    )
    parser.add_argument(
        "--council",
        help="Comma-separated council codes. Default: every council with "
        "results in the old layout",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="List what would happen; write nothing"
    )
    parser.add_argument(
        "--old-prefix", default=OLD_PREFIX, help=f"Where lgsf-web wrote ({OLD_PREFIX})"
    )
    parser.add_argument(
        "--new-prefix",
        default=NEW_PREFIX,
        help=f"Where the pipeline reads ({NEW_PREFIX})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="How many requests to S3 to make at once (default 16)",
    )
    args = parser.parse_args(argv)

    bucket = s3.bucket_from_environment()
    copier = Copier(
        s3.get_client(),
        bucket,
        old_prefix=args.old_prefix,
        new_prefix=args.new_prefix,
        dry_run=args.dry_run,
        workers=args.workers,
    )
    if args.council:
        councils = [
            s3.safe_name(c.strip().upper(), "council")
            for c in args.council.split(",")
            if c.strip()
        ]
    else:
        councils = copier.councils()
    dry = " (dry run)" if args.dry_run else ""
    print(
        f"Copying lgsf-web's results from s3://{bucket}/{copier.old_prefix} to "
        f"s3://{bucket}/{copier.new_prefix} for {len(councils)} councils{dry}"
    )

    reports = []
    for index, council in enumerate(councils, 1):
        report = copier.run_council(council)
        reports.append(report)
        print(
            f"[{index}/{len(councils)}] {council}: {report.statuses:,} statuses, "
            f"{report.copied:,} copied, {report.cheap:,} cheap, "
            f"{report.present:,} present, {report.not_in_merged:,} not in "
            f"{copier.new_prefix}, {report.skipped_failed:,} skipped failed, "
            f"{len(report.problems):,} problems, {len(report.errors):,} errors"
        )
        for problem in report.problems:
            print(f"  problem: {problem}")
        for error in report.errors:
            print(f"  error: {error}")
    print()
    print(summary(reports, args.dry_run))
    return 1 if any(r.failed for r in reports) else 0


if __name__ == "__main__":
    sys.exit(main())
