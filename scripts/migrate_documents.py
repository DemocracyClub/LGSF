"""
Move the decision documents on S3 to content keys.

Decision documents were stored under ``<prefix>/<COUNCIL>/Decisions/documents/``
with keys made from the decision that linked them, such as
``2024-02-28-2199-s100029.pdf``. Every data type now shares
``<prefix>/<COUNCIL>/documents/``, and each file there is named after the
SHA-256 of its bytes and an extension worked out from them (see
lgsf.storage.documents). This copies each old file to its new key and
rewrites the ``storage_key`` recorded against it.

It works on S3 only, and on decisions only, because the old layout only ever
held decision documents. The councils are those with a
``Decisions/_index.json``. It doesn't load scrapers, because a key doesn't
depend on the scraper. Don't run it while a decisions scrape is writing to
the same council: both write ``_index.json``.

Run from the repo root, with the bucket, prefix and AWS credentials set as
for a scrape:

    LGSF_S3_BUCKET=<bucket> LGSF_S3_PREFIX=data AWS_PROFILE=<profile> \\
        uv run python -m scripts.migrate_documents \\
        [--council SWA,MAN] [--dry-run] [--delete-old] [--workers N]

For each council it:

1. Lists the old files.
2. Works out each one's new key from its bytes. A file the listing gives as
   0 bytes is empty and isn't read: a ranged GET of it fails. Otherwise a
   ranged GET of the first 8 KiB gives the bytes the type is sniffed from
   and the object's ``sha256`` metadata. A zip or OLE2 file, or an object
   with no ``sha256`` metadata, is then read whole. The key comes from the
   file, not from the JSON.
3. Writes the mapping of old key, new key, SHA-256, size and ETag to
   ``documents-migration/<COUNCIL>.csv``, before changing anything and
   keeping rows from earlier runs. Once the JSON is rewritten nothing else
   records the old keys.
4. Copies each file inside S3 to ``<COUNCIL>/documents/<new key>``, with its
   ``sha256`` metadata and a ContentType from the new extension, unless the
   key is already there. The same key means the same bytes, so a key that's
   already there is never a clash. CopyObject handles files up to 5 GB.
5. Rewrites the JSON. In every ``Decisions/json/*.json``, then
   ``_index.json``, for documents whose old key is in the mapping and whose
   new key is now listed: ``storage_key`` becomes the new key, and
   ``content_hash`` the file's hash where it disagreed. A document whose
   file is empty loses its storage fields and gets
   ``unavailable: "empty"``, as a scrape would now record it, and its index
   entry is removed. Keys already in the new form are left alone.
6. Checks, from the listing of ``<COUNCIL>/documents/`` taken after
   copying, that every ``storage_key`` in the JSON and index exists with its
   recorded ``content_length``.

``--dry-run`` changes nothing on S3, and reports what a run would do: files
and bytes to copy, how many need a whole-file GET, ``content_hash``
disagreements, JSON naming files that aren't there, and empty files. It
still writes the mapping CSVs, which only record what each old file holds,
so the run after it reuses them rather than reading every file again.

``--delete-old`` is a separate run, once the migration has been checked. It
removes an old file only if the CSV maps it, no JSON or index names it, and
either its new key is listed with the same size or it was empty. Versioning
is off on the bucket, so a deleted file can't be got back.

Rerunning is safe: files already mapped aren't read again, keys already
there aren't copied again, and JSON already rewritten is left alone.

``--to-prefix merged-data`` migrates into a copy instead, leaving the
original prefix exactly as it was for anyone still reading it. Every
council is included, not only those with decisions. Documents go to
``merged-data/<COUNCIL>/documents/<content key>``; decision JSON and the
index are written there with the new keys (or copied as they are, if they
name no old document); and every other file of the council, other data
types and documents already in the new layout included, is copied as it
is. Everything is copied inside S3; the only reads are those of step 2
and the JSON. A rerun copies only what has changed in the original since,
so it can be repeated until everyone has moved over. ``--delete-old``
doesn't apply. A document whose file couldn't be moved is reported, since
its old-layout file isn't in the copy.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from botocore.exceptions import ClientError

from lgsf.storage import s3
from lgsf.storage.documents import is_content_key
from lgsf.storage.documents.sniff import (
    HEAD_BYTES,
    needs_whole_file,
    sniff_extension,
)

MAPPING_DIR = Path("documents-migration")
MAPPING_FIELDS = ["old_key", "new_key", "sha256", "size", "etag"]

#: What a stored document records about its file and the response it came
#: in. A document whose file was empty loses all of them, since a scrape
#: now records only ``unavailable: "empty"`` for one.
STORAGE_FIELDS = (
    "storage_key",
    "storage_backend",
    "content_hash",
    "content_length",
    "etag",
    "last_modified",
    "content_type",
)

#: How many problems of each kind to print for a council. The rest are
#: counted.
SHOW = 5


@dataclass
class Moved:
    """One old file, and the key it goes to: "" for an empty file."""

    old_key: str
    new_key: str
    sha256: str
    size: int
    etag: str = ""


@dataclass
class Read:
    """What working out one old file's new key took."""

    moved: Moved | None
    #: "none" for an empty file, "range" for the first 8 KiB, "whole" when
    #: the whole file had to be read too.
    kind: str = "none"
    error: str | None = None
    warning: str | None = None


@dataclass
class Rewrite:
    """What rewriting the documents of one file found and did."""

    changed: bool = False
    #: (storage_key, content_length) of each document left naming a file.
    named: list = field(default_factory=list)
    #: Old keys still named, which --delete-old must keep.
    old_named: set = field(default_factory=set)
    hash_disagreements: int = 0
    emptied: int = 0


@dataclass
class Report:
    council: str
    dry_run: bool = False
    #: True for a --delete-old run, which reports only what it deletes.
    deleting: bool = False
    old_files: int = 0
    old_bytes: int = 0
    reused: int = 0
    ranged_reads: int = 0
    whole_reads: int = 0
    empty: int = 0
    to_copy: int = 0
    copy_bytes: int = 0
    already_there: int = 0
    json_files: int = 0
    json_rewritten: int = 0
    index_rewritten: bool = False
    hash_disagreements: int = 0
    emptied: int = 0
    deletable: int = 0
    deleted: int = 0
    #: With --to-prefix, the council's other files copied unchanged.
    rest_to_copy: int = 0
    rest_bytes: int = 0
    kept: dict = field(default_factory=dict)
    #: (where, key, what's wrong) for each document naming a file that
    #: isn't there or isn't the size it records.
    problems: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    @property
    def failed(self):
        return bool(self.errors or self.problems)

    def lines(self):
        """The report, one line per fact worth reading."""
        would = "would " if self.dry_run else ""
        lines = [
            f"{self.council}: {self.old_files:,} old files "
            f"({gigabytes(self.old_bytes)})"
        ]
        if self.deleting:
            kept = ", ".join(f"{n:,} {why}" for why, n in sorted(self.kept.items()))
            lines.append(f"  {would}delete {self.deletable:,}; kept {kept or 'none'}")
        else:
            lines.append(
                f"  read: {self.ranged_reads:,} first 8 KiB, {self.whole_reads:,} "
                f"of them whole; {self.reused:,} known from an earlier run; "
                f"{self.empty:,} empty"
            )
            lines.append(
                f"  {would}copy {self.to_copy:,} ({gigabytes(self.copy_bytes)}); "
                f"{self.already_there:,} new keys already there"
            )
            index = " and the index" if self.index_rewritten else ""
            lines.append(
                f"  {would}rewrite {self.json_rewritten:,} of {self.json_files:,} "
                f"decision files{index}"
            )
            if self.rest_to_copy:
                lines.append(
                    f"  {would}copy {self.rest_to_copy:,} other files unchanged "
                    f"({gigabytes(self.rest_bytes)})"
                )
            lines.append(
                f"  decision documents: {self.emptied:,} with an empty file, "
                f'{would}set to unavailable: "empty"; {self.hash_disagreements:,} '
                f"with a content_hash that disagrees with their file"
            )
        if self.problems:
            lines.append(
                f"  {len(self.problems):,} documents name a file that isn't "
                f"there as recorded:"
            )
            lines += [
                f"    {where}: {key}: {why}" for where, key, why in self.problems[:SHOW]
            ]
        for label, items in (("errors", self.errors), ("warnings", self.warnings)):
            if items:
                lines.append(f"  {len(items):,} {label}:")
                lines += [f"    {item}" for item in items[:SHOW]]
        return lines


def gigabytes(n):
    return f"{n / 1e9:,.2f} GB"


def rewrite_document(document, mapping, available, result):
    """
    Point one document, from decision JSON or the index, at its file's new
    key, if that key is ready.

    Returns "emptied" for a document whose file was empty, which the index
    drops, and otherwise "moved" or "kept".
    """
    key = document.get("storage_key")
    if not key:
        return "kept"
    if is_content_key(key):
        result.named.append((key, document.get("content_length")))
        return "kept"

    moved = mapping.get(key)
    if moved is None:
        # Not mapped: the old file isn't there, or couldn't be read.
        result.old_named.add(key)
        result.named.append((key, None))
        return "kept"

    recorded = document.get("content_hash")
    if recorded and recorded != f"sha256:{moved.sha256}":
        result.hash_disagreements += 1

    if not moved.new_key:
        for name in STORAGE_FIELDS:
            document.pop(name, None)
        document["unavailable"] = "empty"
        result.changed = True
        result.emptied += 1
        return "emptied"

    if moved.new_key not in available:
        # Its copy failed: keep naming the old file, which is still there.
        result.old_named.add(key)
        result.named.append((key, None))
        return "kept"

    document["storage_key"] = moved.new_key
    document["content_hash"] = f"sha256:{moved.sha256}"
    result.changed = True
    result.named.append((moved.new_key, document.get("content_length")))
    return "moved"


class Migration:
    def __init__(
        self,
        client,
        bucket,
        prefix="",
        workers=16,
        dry_run=False,
        mapping_dir=MAPPING_DIR,
        out=print,
        to_prefix=None,
    ):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        #: Where to write, when not over the original: see --to-prefix.
        to_prefix = (to_prefix or "").strip("/")
        self.to_prefix = to_prefix if to_prefix and to_prefix != self.prefix else None
        self.workers = workers
        self.dry_run = dry_run
        self.mapping_dir = Path(mapping_dir)
        self.out = out

    # ---- S3 ----

    def key(self, council, *parts):
        return s3.join_key(self.prefix, council, *parts)

    def old_key(self, council, key):
        return self.key(council, "Decisions", "documents", key)

    def new_key(self, council, key):
        return self.dest_key(council, "documents", key)

    def dest_key(self, council, *parts):
        """Where a file goes: under --to-prefix if given, else where it is."""
        return s3.join_key(self.to_prefix or self.prefix, council, *parts)

    def copy_as_is(self, source_key, dest_key):
        """A server-side copy, keeping the object's metadata and type."""
        self.client.copy_object(
            Bucket=self.bucket,
            Key=dest_key,
            CopySource={"Bucket": self.bucket, "Key": source_key},
        )

    def list(self, root):
        """Every key under ``root``, relative to it, with its size and ETag."""
        found = {}
        start = len(root) + 1
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f"{root}/"):
            for item in page.get("Contents", []):
                found[item["Key"][start:]] = {
                    "size": item["Size"],
                    "etag": item.get("ETag", ""),
                }
        return found

    def get(self, key, **kwargs):
        response = self.client.get_object(Bucket=self.bucket, Key=key, **kwargs)
        return response, response["Body"].read()

    def get_json(self, key):
        """A JSON file's content, or None if it isn't there."""
        try:
            _, body = self.get(key)
        except ClientError as e:
            if s3.is_not_found(e):
                return None
            raise
        return json.loads(body)

    def put_json(self, key, data):
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            # As the scrapers write them, so a rewrite changes only the keys.
            Body=json.dumps(data, indent=4, sort_keys=True).encode("utf8"),
            ContentType=s3.content_type(key),
        )

    def councils(self):
        """
        Every council on S3 with a Decisions/_index.json, or with
        --to-prefix every council at all, since all of it is copied.
        """
        prefix = f"{self.prefix}/" if self.prefix else ""
        paginator = self.client.get_paginator("list_objects_v2")
        found = []
        for page in paginator.paginate(
            Bucket=self.bucket, Prefix=prefix, Delimiter="/"
        ):
            for common in page.get("CommonPrefixes", []):
                found.append(common["Prefix"][len(prefix) :].strip("/"))
        if self.to_prefix:
            return sorted(found)
        return [
            council
            for council in sorted(found)
            if self.exists(self.key(council, "Decisions", "_index.json"))
        ]

    def exists(self, key):
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if s3.is_not_found(e):
                return False
            raise
        return True

    def in_parallel(self, fn, items):
        """
        ``fn`` over ``items`` in ``workers`` threads, in order. A call that
        raises gives the exception in place of its result, so one bad file
        doesn't stop a council.
        """

        def call(item):
            try:
                return fn(item)
            except Exception as e:
                return e

        with ThreadPoolExecutor(self.workers) as pool:
            return list(pool.map(call, items))

    # ---- the mapping ----

    def mapping_path(self, council):
        return self.mapping_dir / f"{council}.csv"

    def load_mapping(self, council):
        path = self.mapping_path(council)
        if not path.exists():
            return {}
        with path.open(newline="") as f:
            return {
                row["old_key"]: Moved(
                    old_key=row["old_key"],
                    new_key=row["new_key"],
                    sha256=row["sha256"],
                    size=int(row["size"]),
                    etag=row["etag"],
                )
                for row in csv.DictReader(f)
            }

    def save_mapping(self, council, mapping):
        self.mapping_dir.mkdir(parents=True, exist_ok=True)
        path = self.mapping_path(council)
        tmp = path.with_name(f".{path.name}.tmp")
        with tmp.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=MAPPING_FIELDS)
            writer.writeheader()
            for old_key in sorted(mapping):
                moved = mapping[old_key]
                writer.writerow(
                    {
                        "old_key": moved.old_key,
                        "new_key": moved.new_key,
                        "sha256": moved.sha256,
                        "size": moved.size,
                        "etag": moved.etag,
                    }
                )
        tmp.replace(path)

    # ---- step 2 ----

    def identify(self, council, old_key, listed):
        """Work out one old file's new key from its bytes."""
        size, etag = listed["size"], listed["etag"]
        if size == 0:
            empty = hashlib.sha256(b"").hexdigest()
            return Read(Moved(old_key, "", empty, 0, etag), "none")

        key = self.old_key(council, old_key)
        response, content = self.get(key, Range=f"bytes=0-{HEAD_BYTES - 1}")
        digest = (response.get("Metadata") or {}).get("sha256")
        kind = "range"
        whole = len(content) >= size
        if not whole and (needs_whole_file(content) or not digest):
            _, content = self.get(key)
            kind = "whole"
            whole = True

        warning = None
        if whole:
            actual = hashlib.sha256(content).hexdigest()
            if digest and digest != actual:
                warning = (
                    f"{old_key}: sha256 metadata {digest} but the bytes hash "
                    f"to {actual}; using the bytes"
                )
            digest = actual
            size = len(content)

        new_key = digest + sniff_extension(content)
        return Read(Moved(old_key, new_key, digest, size, etag), kind, warning=warning)

    # ---- the steps, for one council ----

    def migrate(self, council):
        report = Report(council, dry_run=self.dry_run)

        # 1. List the old files.
        old = self.list(self.key(council, "Decisions", "documents"))
        report.old_files = len(old)
        report.old_bytes = sum(item["size"] for item in old.values())

        # 2. Work out each one's new key, reusing what an earlier run found
        # for a file that hasn't changed since.
        mapping = self.load_mapping(council)
        to_read = []
        for old_key, listed in old.items():
            known = mapping.get(old_key)
            if known and known.size == listed["size"] and known.etag == listed["etag"]:
                report.reused += 1
            else:
                to_read.append(old_key)
        if to_read:
            self.out(f"{council}: reading {len(to_read):,} files")
        for old_key, read in zip(
            to_read,
            self.in_parallel(lambda k: self.identify(council, k, old[k]), to_read),
        ):
            if isinstance(read, Exception):
                report.errors.append(f"reading {old_key}: {read}")
                continue
            mapping[old_key] = read.moved
            if read.kind == "range":
                report.ranged_reads += 1
            elif read.kind == "whole":
                report.ranged_reads += 1
                report.whole_reads += 1
            if read.warning:
                report.warnings.append(read.warning)
        report.empty = sum(1 for k in old if k in mapping and not mapping[k].new_key)

        # 3. Record the mapping before changing anything.
        self.save_mapping(council, mapping)

        # 4. Copy each file to its new key, once per key.
        new = self.list(self.dest_key(council, "documents"))
        targets = {}
        for old_key in old:
            moved = mapping.get(old_key)
            if moved and moved.new_key:
                targets.setdefault(moved.new_key, moved)
        to_copy = [m for key, m in targets.items() if key not in new]
        report.to_copy = len(to_copy)
        report.copy_bytes = sum(m.size for m in to_copy)
        report.already_there = len(targets) - len(to_copy)

        if self.dry_run:
            available = {key: item["size"] for key, item in new.items()}
            available.update({m.new_key: m.size for m in to_copy})
        else:
            if to_copy:
                self.out(f"{council}: copying {len(to_copy):,} files")
            for moved, result in zip(
                to_copy, self.in_parallel(lambda m: self.copy(council, m), to_copy)
            ):
                if isinstance(result, Exception):
                    report.errors.append(f"copying {moved.old_key}: {result}")
            # One listing, after copying, both for what may be named and
            # for the check: what is really there, not what should be.
            available = {
                key: item["size"]
                for key, item in self.list(self.dest_key(council, "documents")).items()
            }

        # 5. Rewrite the JSON, the index last.
        named = []
        json_root = self.key(council, "Decisions", "json")
        json_keys = [k for k in self.list(json_root) if k.endswith(".json")]
        report.json_files = len(json_keys)
        if json_keys:
            self.out(f"{council}: rewriting {len(json_keys):,} decision files")

        dest_json_root = self.dest_key(council, "Decisions", "json")

        def rewrite_decision(json_key):
            key = s3.join_key(json_root, json_key)
            dest = s3.join_key(dest_json_root, json_key)
            data = self.get_json(key)
            result = Rewrite()
            for document in (data or {}).get("documents") or []:
                rewrite_document(document, mapping, available, result)
            if not self.dry_run:
                if result.changed:
                    self.put_json(dest, data)
                elif dest != key:
                    self.copy_as_is(key, dest)
            return result

        for json_key, result in zip(
            json_keys, self.in_parallel(rewrite_decision, json_keys)
        ):
            if isinstance(result, Exception):
                report.errors.append(f"rewriting {json_key}: {result}")
                continue
            report.json_rewritten += result.changed
            report.hash_disagreements += result.hash_disagreements
            report.emptied += result.emptied
            named += [(f"json/{json_key}", *n) for n in result.named]

        index_key = self.key(council, "Decisions", "_index.json")
        index = self.get_json(index_key)
        if index is not None:
            result = Rewrite()
            documents = index.get("documents") or {}
            for url, entry in list(documents.items()):
                if rewrite_document(entry, mapping, available, result) == "emptied":
                    del documents[url]
            named += [("_index.json", *n) for n in result.named]
            dest_index_key = self.dest_key(council, "Decisions", "_index.json")
            if result.changed:
                report.index_rewritten = True
                if not self.dry_run:
                    self.put_json(dest_index_key, index)
            elif dest_index_key != index_key and not self.dry_run:
                self.copy_as_is(index_key, dest_index_key)

        # 6. Check every file named is there, at the size recorded.
        old_sizes = {k: item["size"] for k, item in old.items()}
        for where, key, length in named:
            if is_content_key(key):
                size = available.get(key)
            elif self.to_prefix:
                # Still an old-layout key, and those aren't copied
                report.problems.append(
                    (where, key, f"old-layout file, not in {self.to_prefix}/")
                )
                continue
            else:
                size = old_sizes.get(key)
            if size is None:
                report.problems.append((where, key, "not there"))
            elif length is not None and size != length:
                report.problems.append(
                    (where, key, f"{size:,} bytes, recorded as {length:,}")
                )

        # 7. With --to-prefix, everything else the council has.
        if self.to_prefix:
            self.copy_rest(council, report)
        return report

    def copy_rest(self, council, report):
        """
        Copy, server-side and unchanged, every file of the council's not
        handled above: other data types, documents already in the new
        layout, run logs. Not the old decision documents, which went to
        content keys, nor the decision JSON and index, rewritten above. A
        file already there with the same size and ETag is skipped, so a
        rerun copies only what changed since.
        """
        source = self.list(self.key(council))
        dest = self.list(self.dest_key(council))
        handled = ("Decisions/documents/", "Decisions/json/")
        to_copy = [
            key
            for key, item in source.items()
            if not key.startswith(handled)
            and key != "Decisions/_index.json"
            and dest.get(key) != item
        ]
        report.rest_to_copy = len(to_copy)
        report.rest_bytes = sum(source[k]["size"] for k in to_copy)
        if self.dry_run or not to_copy:
            return
        self.out(f"{council}: copying {len(to_copy):,} other files")
        for key, result in zip(
            to_copy,
            self.in_parallel(
                lambda k: self.copy_as_is(
                    self.key(council, k), self.dest_key(council, k)
                ),
                to_copy,
            ),
        ):
            if isinstance(result, Exception):
                report.errors.append(f"copying {key}: {result}")

    def copy(self, council, moved):
        self.client.copy_object(
            Bucket=self.bucket,
            Key=self.new_key(council, moved.new_key),
            CopySource={
                "Bucket": self.bucket,
                "Key": self.old_key(council, moved.old_key),
            },
            MetadataDirective="REPLACE",
            Metadata={"sha256": moved.sha256},
            ContentType=s3.content_type(moved.new_key),
        )

    # ---- --delete-old ----

    def delete_old(self, council):
        """Delete the old files nothing needs any more. See the module."""
        report = Report(council, dry_run=self.dry_run, deleting=True)
        old = self.list(self.key(council, "Decisions", "documents"))
        report.old_files = len(old)
        report.old_bytes = sum(item["size"] for item in old.values())
        mapping = self.load_mapping(council)
        new = {
            key: item["size"]
            for key, item in self.list(self.key(council, "documents")).items()
        }

        # Every old key still named by the JSON or the index.
        still_named = set()
        json_root = self.key(council, "Decisions", "json")
        json_keys = [k for k in self.list(json_root) if k.endswith(".json")]
        report.json_files = len(json_keys)

        def old_keys_named(json_key):
            data = self.get_json(s3.join_key(json_root, json_key)) or {}
            return {
                d.get("storage_key")
                for d in data.get("documents") or []
                if d.get("storage_key")
            }

        for json_key, keys in zip(
            json_keys, self.in_parallel(old_keys_named, json_keys)
        ):
            if isinstance(keys, Exception):
                # Can't tell what it names, so nothing it might name can go.
                report.errors.append(f"reading {json_key}: {keys}")
                return report
            still_named |= keys
        index = self.get_json(self.key(council, "Decisions", "_index.json")) or {}
        still_named |= {
            e.get("storage_key") for e in (index.get("documents") or {}).values()
        }

        doomed = []
        for old_key, listed in sorted(old.items()):
            moved = mapping.get(old_key)
            if moved is None:
                why = "not in the mapping"
            elif moved.size != listed["size"] or moved.etag != listed["etag"]:
                why = "changed since it was mapped"
            elif old_key in still_named:
                why = "still named"
            elif moved.new_key and new.get(moved.new_key) != moved.size:
                why = "new key not there at that size"
            else:
                doomed.append(old_key)
                continue
            report.kept[why] = report.kept.get(why, 0) + 1
        report.deletable = len(doomed)

        if not self.dry_run:
            for start in range(0, len(doomed), 1000):
                batch = doomed[start : start + 1000]
                response = self.client.delete_objects(
                    Bucket=self.bucket,
                    Delete={
                        "Objects": [{"Key": self.old_key(council, k)} for k in batch],
                        "Quiet": True,
                    },
                )
                failed = (response or {}).get("Errors") or []
                report.errors += [
                    f"deleting {e['Key']}: {e.get('Message')}" for e in failed
                ]
                report.deleted += len(batch) - len(failed)
        return report


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m scripts.migrate_documents",
        description="Move the decision documents on S3 to content keys.",
    )
    parser.add_argument(
        "--council", help="Comma-separated council codes. Default: every council"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Report, but change nothing on S3"
    )
    parser.add_argument(
        "--delete-old",
        action="store_true",
        help="Delete the old files that have been moved and nothing names",
    )
    parser.add_argument(
        "--to-prefix",
        help="Write the migrated data under this prefix instead of over "
        "the original, which is then left exactly as it was: documents go "
        "to <to-prefix>/<COUNCIL>/documents/, decision JSON and index are "
        "written there rewritten, and everything else is copied unchanged. "
        "Every council is included, not only those with decisions.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="How many requests to S3 to make at once (default 16)",
    )
    args = parser.parse_args(argv)
    if args.to_prefix and args.delete_old:
        parser.error("--delete-old doesn't apply with --to-prefix: nothing is moved")

    bucket = s3.bucket_from_environment()
    prefix = s3.prefix_from_environment()
    migration = Migration(
        s3.get_client(),
        bucket,
        prefix,
        workers=args.workers,
        dry_run=args.dry_run,
        to_prefix=args.to_prefix,
    )

    if args.council:
        councils = [s3.safe_name(c.strip(), "council") for c in args.council.split(",")]
    else:
        councils = migration.councils()

    action = "Deleting old documents" if args.delete_old else "Moving documents"
    dry = " (dry run)" if args.dry_run else ""
    where = f"s3://{bucket}/{prefix}"
    if migration.to_prefix:
        where += f" to s3://{bucket}/{migration.to_prefix}"
    print(f"{action} in {where} for {len(councils)} councils{dry}")

    failed = []
    for council in councils:
        try:
            if args.delete_old:
                report = migration.delete_old(council)
            else:
                report = migration.migrate(council)
        except Exception as e:
            print(f"{council}: failed: {type(e).__name__}: {e}", flush=True)
            failed.append(council)
            continue
        print("\n".join(report.lines()), flush=True)
        if report.failed:
            failed.append(council)

    if failed:
        print(f"Councils with errors or problems: {', '.join(failed)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
