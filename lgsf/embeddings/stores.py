"""
Where documents and the files written beside them are read from and
written to: the local data directory, or the bucket it's a copy of.

Both stores take keys relative to the data directory, such as
``STO/documents/<sha256>.pdf`` or ``STO/Minutes/json/<record>.json``.
In the bucket, that's under LGSF_S3_PREFIX, as lgsf.storage.s3 lays it
out.

A store is handed to worker processes, so S3Store leaves its boto3
client out when it's pickled and gets one again when it's first used.
"""

import datetime
import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from uuid import uuid4

from lgsf.conf import settings
from lgsf.storage import s3


class Source(StrEnum):
    LOCAL = "local"
    S3 = "s3"


@dataclass(frozen=True)
class Entry:
    """What a listing says about one key."""

    size: int
    #: When the object was last written, in UTC.
    last_modified: datetime.datetime


class LocalStore:
    def __init__(self, root: Path):
        self.root = Path(root)

    def __repr__(self):
        return f"LocalStore({str(self.root)!r})"

    def path(self, key: str) -> Path:
        return self.root / key

    def exists(self, key: str) -> bool:
        return self.path(key).is_file()

    def list_prefix(self, prefix: str) -> list[str]:
        """
        Every key starting with ``prefix``. Only a directory's files are
        listed, not those in directories below it.
        """
        return sorted(self.list_sizes(prefix))

    def list_sizes(self, prefix: str) -> dict[str, int]:
        """The size in bytes of every file list_prefix() lists."""
        return {key: entry.size for key, entry in self.list_entries(prefix).items()}

    def list_entries(self, prefix: str) -> dict[str, Entry]:
        """The size and modification time of every file list_prefix() lists."""
        directory, _, start = prefix.rpartition("/")
        path = self.path(directory)
        if not path.is_dir():
            return {}
        entries = {}
        for entry in os.scandir(path):
            if not (entry.is_file() and entry.name.startswith(start)):
                continue
            stat = entry.stat()
            key = f"{directory}/{entry.name}" if directory else entry.name
            entries[key] = Entry(
                size=stat.st_size,
                last_modified=datetime.datetime.fromtimestamp(
                    stat.st_mtime, datetime.UTC
                ),
            )
        return entries

    def read_bytes(self, key: str) -> bytes:
        """Raises FileNotFoundError if there's nothing at ``key``."""
        return self.path(key).read_bytes()

    def write_bytes(self, key: str, data: bytes):
        """
        Written to a temporary file that's then renamed over the old one,
        so a run that's stopped never leaves a file half written.
        """
        path = self.path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f".tmp-{uuid4().hex}-{path.name}")
        try:
            temp.write_bytes(data)
            temp.replace(path)
        except OSError:
            temp.unlink(missing_ok=True)
            raise

    def download_to(self, key: str, directory: Path) -> Path:
        """The file's own path: nothing needs downloading."""
        path = self.path(key)
        if not path.is_file():
            raise FileNotFoundError(path)
        return path


class S3Store:
    def __init__(
        self, bucket: str | None = None, prefix: str | None = None, client=None
    ):
        self.bucket = s3.bucket_from_environment(bucket)
        self.prefix = s3.prefix_from_environment(prefix)
        self._client = client

    def __repr__(self):
        return f"S3Store({self.bucket!r}, {self.prefix!r})"

    def __getstate__(self):
        return {**self.__dict__, "_client": None}

    @property
    def client(self):
        if self._client is None:
            self._client = s3.get_client()
        return self._client

    def object_key(self, key: str) -> str:
        return s3.join_key(self.prefix, key)

    def url(self, key: str) -> str:
        return f"s3://{self.bucket}/{self.object_key(key)}"

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=self.object_key(key))
        except ClientError as error:
            if s3.is_not_found(error):
                return False
            raise
        return True

    def list_prefix(self, prefix: str) -> list[str]:
        """
        Every key starting with ``prefix``, including any under
        "directories" below it, as S3 has none.
        """
        return sorted(self.list_sizes(prefix))

    def list_sizes(self, prefix: str) -> dict[str, int]:
        """The size in bytes of every object list_prefix() lists."""
        return {key: entry.size for key, entry in self.list_entries(prefix).items()}

    def list_entries(self, prefix: str) -> dict[str, Entry]:
        """
        The size and LastModified of every object list_prefix() lists.
        One ListObjectsV2 request for each thousand keys.

        A trailing slash on ``prefix`` is kept: "STO/documents/" must not
        list "STO/documents-backup/".
        """
        start = len(self.prefix) + 1 if self.prefix else 0
        key_prefix = self.object_key(prefix)
        if prefix.endswith("/") or not prefix:
            key_prefix += "/"
        entries = {}
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=key_prefix):
            for item in page.get("Contents", []):
                entries[item["Key"][start:]] = Entry(
                    size=item.get("Size", 0),
                    last_modified=item["LastModified"].astimezone(datetime.UTC),
                )
        return entries

    def read_bytes(self, key: str) -> bytes:
        """Raises FileNotFoundError if there's nothing at ``key``."""
        from botocore.exceptions import ClientError

        try:
            response = self.client.get_object(
                Bucket=self.bucket, Key=self.object_key(key)
            )
        except ClientError as error:
            if s3.is_not_found(error):
                raise FileNotFoundError(self.url(key)) from None
            raise
        return response["Body"].read()

    def write_bytes(self, key: str, data: bytes):
        """A PUT is written whole or not at all."""
        self.client.put_object(
            Bucket=self.bucket,
            Key=self.object_key(key),
            Body=data,
            ContentType=s3.content_type(key),
        )

    def download_to(self, key: str, directory: Path) -> Path:
        """
        Streamed to disk, so a large document is never held in memory.
        Raises FileNotFoundError if there's nothing at ``key``.
        """
        from botocore.exceptions import ClientError

        path = Path(directory) / Path(key).name
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.client.download_file(self.bucket, self.object_key(key), str(path))
        except ClientError as error:
            if s3.is_not_found(error):
                raise FileNotFoundError(self.url(key)) from None
            raise
        return path


def store_for(source: Source, root: Path | None = None):
    """
    The store ``--source`` names: the local data directory, or the
    bucket and prefix LGSF_S3_BUCKET and LGSF_S3_PREFIX name.
    """
    if Source(source) == Source.S3:
        return S3Store()
    return LocalStore(root or settings.BASE_PATH / settings.DATA_DIR_NAME)
