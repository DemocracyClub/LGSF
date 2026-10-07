"""S3 document storage: the local document store, in a bucket."""

from __future__ import annotations

import hashlib
from typing import Optional

from botocore.exceptions import ClientError

from lgsf.storage import s3
from lgsf.storage.documents.base import BaseDocumentStorage, StoredDocument


class S3DocumentStorage(BaseDocumentStorage):
    """
    Stores documents under ``s3://<bucket>/<prefix>/<COUNCIL>/documents/``,
    alongside the council's metadata just as data/<COUNCIL>/documents/ is
    locally.

    Each object carries its SHA-256 in its metadata, so describing a stored
    document is a HEAD request rather than downloading it to hash it.
    """

    name = "s3"

    def __init__(
        self,
        council_code: str,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        client=None,
    ):
        super().__init__(council_code)
        self.bucket = s3.bucket_from_environment(bucket)
        self.prefix = s3.prefix_from_environment(prefix)
        self.client = client or s3.get_client()
        self.root = s3.join_key(
            self.prefix, s3.safe_name(self.council_code, "council_code"), "documents"
        )
        self.listing = s3.KeyListing(self.client, self.bucket, self.root)

    def object_key(self, key: str) -> str:
        return s3.join_key(self.root, s3.relative_key(key))

    def exists(self, key: str) -> bool:
        return s3.relative_key(key) in self.listing

    def write(self, key: str, content: bytes) -> StoredDocument:
        digest = hashlib.sha256(content).hexdigest()
        self.client.put_object(
            Bucket=self.bucket,
            Key=self.object_key(key),
            Body=content,
            ContentType=s3.content_type(key),
            Metadata={"sha256": digest},
        )
        self.listing.add(s3.relative_key(key))
        return StoredDocument(
            key=key,
            url=self.url_for(key),
            content_hash=f"sha256:{digest}",
            content_length=len(content),
            backend=self.name,
        )

    def read(self, key: str) -> bytes:
        try:
            response = self.client.get_object(
                Bucket=self.bucket, Key=self.object_key(key)
            )
        except ClientError as e:
            if s3.is_not_found(e):
                raise FileNotFoundError(self.url_for(key)) from e
            raise
        return response["Body"].read()

    def url_for(self, key: str) -> str:
        return f"s3://{self.bucket}/{self.object_key(key)}"

    def describe(self, key: str) -> StoredDocument | None:
        if not self.exists(key):
            return None
        try:
            head = self.client.head_object(Bucket=self.bucket, Key=self.object_key(key))
        except ClientError as e:
            if s3.is_not_found(e):
                return None
            raise
        digest = (head.get("Metadata") or {}).get("sha256")
        if not digest:
            # Put there by something other than this backend: hash it the
            # slow way.
            return super().describe(key)
        return StoredDocument(
            key=key,
            url=self.url_for(key),
            content_hash=f"sha256:{digest}",
            content_length=head["ContentLength"],
            backend=self.name,
        )
