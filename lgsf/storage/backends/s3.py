"""
S3 metadata storage: the local backend, with a bucket in place of data/.

A session stages writes in memory exactly as the local one does, and ending
it uploads them. That makes a checkpoint cheap here too, so a long run's
progress reaches S3 every checkpoint, and each upload carries records and
the index that names them together: someone syncing mid-run never sees an
index pointing at files that haven't arrived. See lgsf.storage.s3 for the
layout and configuration.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Literal, Optional, Union

from botocore.exceptions import ClientError

from lgsf.storage import s3
from lgsf.storage.backends.base import BaseStorage, StorageMode, StorageSession


class _S3Session(StorageSession):
    """
    Writes staged in memory until the session ends; reads see staged
    content first, then S3. Not thread-safe, like the local session.
    """

    def __init__(self, storage: "S3Storage", encoding: str = "utf-8"):
        self._storage = storage
        self._encoding = encoding
        self._staged: Dict[str, bytes] = {}
        self._closed = False

    def write(self, filename: Path, content: str) -> None:
        self._assert_open()
        self._staged[s3.relative_key(filename)] = content.encode(self._encoding)

    def write_bytes(self, filename: Path, content: bytes) -> None:
        self._assert_open()
        self._staged[s3.relative_key(filename)] = content

    def touch(self, filename: Path) -> None:
        self._assert_open()
        self._staged[s3.relative_key(filename)] = b""

    def open(self, filename: Path, mode: Literal["r", "rb"] = "r") -> Union[str, bytes]:
        self._assert_open()
        key = s3.relative_key(filename)
        if key in self._staged:
            data = self._staged[key]
        else:
            data = self._storage.get(key)
        return data if mode == "rb" else data.decode(self._encoding)

    def exists(self, filename: Path) -> bool:
        self._assert_open()
        key = s3.relative_key(filename)
        return key in self._staged or key in self._storage.listing

    def _consume_staged(self) -> Dict[str, bytes]:
        self._assert_open()
        staged = self._staged
        self._staged = {}
        self._closed = True
        return staged

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("Session is closed.")


class S3Storage(BaseStorage):
    """
    Stores one council's data of one type under
    ``s3://<bucket>/<prefix>/<COUNCIL>/<Type>/``.

    In REPLACE mode, ending a session also deletes whatever is under that
    prefix that the session didn't write, which is what the local backend's
    clearing of the directory amounts to, but done after the new data is in
    place rather than before, so a failed run leaves the old data alone.
    The documents/ beside the metadata belong to S3DocumentStorage and are
    never deleted here.
    """

    supports_checkpoints = True

    #: Uploads run in parallel: a checkpoint is a hundred-odd small files.
    upload_workers = 8

    def __init__(
        self,
        council_code: str,
        scraper_object_type: Optional[str] = None,
        storage_mode: StorageMode = StorageMode.REPLACE,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        client=None,
    ):
        super().__init__(council_code, storage_mode=storage_mode)
        self.bucket = s3.bucket_from_environment(bucket)
        self.prefix = s3.prefix_from_environment(prefix)
        self.client = client or s3.get_client()
        self.encoding = "utf8"
        self._active: Optional[_S3Session] = None

        parts = [self.prefix, s3.safe_name(council_code, "council_code")]
        if scraper_object_type:
            parts.append(s3.safe_name(scraper_object_type, "scraper_object_type"))
        self.root = s3.join_key(*parts)
        self.listing = s3.KeyListing(self.client, self.bucket, self.root)

    def object_key(self, key: str) -> str:
        return s3.join_key(self.root, key)

    def url_for(self, key: str) -> str:
        return f"s3://{self.bucket}/{self.object_key(key)}"

    def get(self, key: str) -> bytes:
        try:
            response = self.client.get_object(
                Bucket=self.bucket, Key=self.object_key(key)
            )
        except ClientError as e:
            if s3.is_not_found(e):
                raise FileNotFoundError(self.url_for(key)) from e
            raise
        return response["Body"].read()

    def put(self, key: str, data: bytes) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=self.object_key(key),
            Body=data,
            ContentType=s3.content_type(key),
        )
        self.listing.add(key)

    def write_now(self, key: str, content: str) -> None:
        """
        Write one file straight to S3, outside any session. For records
        that must land whether or not the run's session commits, such as
        the run log of a run that failed.
        """
        self.put(s3.relative_key(key), content.encode(self.encoding))

    def _start_session(self, **kwargs) -> StorageSession:
        if self._active is not None:
            raise RuntimeError("A session is already active on this S3Storage.")
        session = _S3Session(self, encoding=self.encoding)
        self._active = session
        return session

    def _end_session(self, session: StorageSession, commit_message: str, **kwargs):
        if not isinstance(session, _S3Session) or session is not self._active:
            raise RuntimeError("Unknown or inactive session for this S3Storage.")
        if not commit_message or not commit_message.strip():
            raise ValueError("commit_message cannot be empty")

        try:
            staged = session._consume_staged()
            if not staged:
                return {"skipped": True, "reason": "no changes"}

            run_log = kwargs.get("run_log")
            if run_log:
                staged["scrape_summary.json"] = self._summary(
                    commit_message, len(staged), run_log
                )

            # Bookkeeping files - the index, _last-run - go up only once
            # everything else has, so an index on S3 never names a file
            # that isn't there yet, for a later run or for someone syncing
            # mid-run. An upload failing part way leaves earlier files in
            # place and the index unchanged, so they are fetched again.
            data = {k: v for k, v in staged.items() if not k.startswith("_")}
            bookkeeping = {k: v for k, v in staged.items() if k.startswith("_")}
            with ThreadPoolExecutor(self.upload_workers) as pool:
                list(pool.map(lambda item: self.put(*item), data.items()))
            for key, value in bookkeeping.items():
                self.put(key, value)

            deleted = []
            if self.storage_mode == StorageMode.REPLACE:
                deleted = self._delete_all_but(set(staged))

            return {
                "applied": len(staged),
                "root": f"s3://{self.bucket}/{self.root}",
                "files": [self.url_for(key) for key in staged],
                "deleted": len(deleted),
                "commit_message": commit_message.strip(),
            }
        finally:
            self._reset_session_state(session)

    def _summary(self, commit_message, files_written, run_log) -> bytes:
        """The scrape_summary.json the local backend writes alongside."""
        if not getattr(run_log, "finished", True):
            run_log.finish()
        summary = {
            "council": self.council_code,
            "commit_message": commit_message.strip(),
            "files_written": files_written,
            "summary": "S3 scrape completed",
        }
        try:
            summary.update(run_log.as_dict)
        except (AttributeError, TypeError, ValueError):
            pass
        return json.dumps(summary, indent=2, default=str).encode(self.encoding)

    def _delete_all_but(self, keep):
        documents = f"{s3.DOCUMENTS_DIR}/"
        stale = sorted(
            key
            for key in set(self.listing.keys) - keep
            if not key.startswith(documents)
        )
        for start in range(0, len(stale), 1000):
            batch = stale[start : start + 1000]
            self.client.delete_objects(
                Bucket=self.bucket,
                Delete={
                    "Objects": [{"Key": self.object_key(k)} for k in batch],
                    "Quiet": True,
                },
            )
            for key in batch:
                self.listing.discard(key)
        return stale

    def _reset_session_state(self, session: Optional[StorageSession]) -> None:
        self._active = None
