"""Local filesystem document storage: the default backend."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from lgsf.conf import settings
from lgsf.storage.documents.base import (
    BaseDocumentStorage,
    StoredDocument,
    hash_content,
)


class LocalDocumentStorage(BaseDocumentStorage):
    """
    Stores documents on the local filesystem under
    ``data/<COUNCIL_ID>/documents/``.

    Sitting alongside (not inside) the per-data-type metadata directories
    means the same document store is shared by every scraper type for a
    council, and that ``data/<COUNCIL_ID>/documents/`` can be excluded from
    version control while the metadata beside it is committed.

    Writes go via a temporary file and an atomic rename, so a crashed or
    interrupted run can never leave a half-written document that a later
    run would mistake for a complete one.
    """

    name = "local"

    def __init__(self, council_code: str):
        super().__init__(council_code)

        safe_council_code = "".join(
            c for c in self.council_code if c.isalnum() or c in "_-"
        )
        if not safe_council_code:
            raise ValueError(f"Invalid council_code: {council_code}")

        self.root = (
            Path(settings.DATA_DIR_NAME) / safe_council_code / "documents"
        ).resolve()

    def _path(self, key: str) -> Path:
        """Resolve ``key`` to a path, refusing anything outside the root."""
        if not key or not key.strip():
            raise ValueError("Document key cannot be empty")

        candidate = Path(key)
        if candidate.is_absolute():
            raise ValueError(f"Absolute keys not allowed: {key}")
        if ".." in candidate.parts:
            raise ValueError(f"Path traversal not allowed: {key}")

        resolved = (self.root / candidate).resolve()
        if not resolved.is_relative_to(self.root):
            raise ValueError(f"Key {key} resolves outside {self.root}")
        return resolved

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def write(self, key: str, content: bytes) -> StoredDocument:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)

        tmp = target.with_name(f".tmp-{uuid4().hex}-{target.name}")
        try:
            with tmp.open("xb") as f:
                f.write(content)
                f.flush()
            tmp.replace(target)
        except OSError:
            tmp.unlink(missing_ok=True)
            raise

        return StoredDocument(
            key=key,
            url=self.url_for(key),
            content_hash=hash_content(content),
            content_length=len(content),
            backend=self.name,
        )

    def read(self, key: str) -> bytes:
        path = self._path(key)
        if not path.is_file():
            raise FileNotFoundError(str(path))
        return path.read_bytes()

    def url_for(self, key: str) -> str:
        return self._path(key).as_uri()
