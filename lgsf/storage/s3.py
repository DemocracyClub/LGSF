"""
What the S3 metadata and document backends share: where to write, a client,
and a listing of what is already there.

Everything for one data type of one council sits under one prefix, since
every data type shares the bucket:

    <prefix>/<COUNCIL>/<Type>/json/...         metadata (S3Storage)
    <prefix>/<COUNCIL>/<Type>/_index.json
    <prefix>/<COUNCIL>/<Type>/documents/...    documents (S3DocumentStorage)

The metadata is laid out as in the local data directory, so
``aws s3 sync s3://<bucket>/<prefix> data/`` gives a data directory LGSF can
read. Documents differ: locally they are shared by every type at
data/<COUNCIL>/documents/.

Configured with LGSF_S3_BUCKET and LGSF_S3_PREFIX. Credentials and region
come from the usual AWS chain: AWS_PROFILE, environment, instance role.
"""

from __future__ import annotations

import mimetypes
import os
import threading
from pathlib import PurePosixPath

_client = None
_client_lock = threading.Lock()


def get_client():
    """
    One S3 client for the process.

    boto3 clients are thread-safe once made, and a run over many councils
    runs them in threads, so they share one rather than each opening a
    connection pool of its own. Making one is not thread-safe, hence the
    lock.
    """
    global _client
    with _client_lock:
        if _client is None:
            import boto3
            from botocore.config import Config

            _client = boto3.client(
                "s3",
                config=Config(
                    retries={"max_attempts": 10, "mode": "adaptive"},
                    max_pool_connections=50,
                ),
            )
    return _client


def bucket_from_environment(bucket=None):
    bucket = bucket or os.environ.get("LGSF_S3_BUCKET")
    if not bucket:
        raise ValueError(
            "S3 storage needs a bucket: set LGSF_S3_BUCKET to the bucket name"
        )
    return bucket


def prefix_from_environment(prefix=None):
    if prefix is None:
        prefix = os.environ.get("LGSF_S3_PREFIX", "")
    return prefix.strip("/")


#: The directory under a council's type that holds its documents. The
#: metadata store leaves it alone.
DOCUMENTS_DIR = "documents"


def safe_name(value, what):
    """The same sanitising the local backends apply to directory names."""
    safe = "".join(c for c in value if c.isalnum() or c in "_-")
    if not safe:
        raise ValueError(f"Invalid {what}: {value}")
    return safe


def join_key(*parts):
    return "/".join(str(p).strip("/") for p in parts if p and str(p).strip("/"))


def relative_key(name):
    """
    Turn a path inside a council's area into a key, refusing the same
    things the local backends refuse.
    """
    path = PurePosixPath(str(name))
    if path.is_absolute():
        raise ValueError(f"Absolute paths not allowed: {name}")
    if ".." in path.parts:
        raise ValueError(f"Path traversal not allowed: {name}")
    key = path.as_posix().lstrip("/")
    if not key or key == ".":
        raise ValueError("Empty path not allowed")
    return key


def content_type(name):
    """A content type, so that files open properly from the S3 console."""
    guessed, _ = mimetypes.guess_type(str(name))
    if guessed is None:
        return "application/octet-stream"
    if guessed.startswith("text/") or guessed == "application/json":
        return f"{guessed}; charset=utf-8"
    return guessed


def is_not_found(error):
    """True for the ways S3 says an object isn't there."""
    code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
    return code in ("NoSuchKey", "404", "NotFound")


class KeyListing:
    """
    The keys under one prefix, listed once and then kept up to date.

    Scrapers ask "do we already have this?" for every record and document
    they consider, which locally is a cheap stat. Against S3 it would be a
    request each, thousands per council per run, so the prefix is listed
    the first time it is asked about and answered from memory after that.
    Keys written through the backend are added as they go.
    """

    def __init__(self, client, bucket, root):
        self.client = client
        self.bucket = bucket
        self.root = root
        self._keys = None

    def _load(self):
        keys = set()
        start = len(self.root) + 1 if self.root else 0
        paginator = self.client.get_paginator("list_objects_v2")
        prefix = f"{self.root}/" if self.root else ""
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                keys.add(item["Key"][start:])
        return keys

    @property
    def keys(self):
        if self._keys is None:
            self._keys = self._load()
        return self._keys

    def __contains__(self, key):
        return key in self.keys

    def add(self, key):
        if self._keys is not None:
            self._keys.add(key)

    def discard(self, key):
        if self._keys is not None:
            self._keys.discard(key)
