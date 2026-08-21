"""
Pluggable storage for scraped document *files*.

This is deliberately separate from ``lgsf.storage.backends``, which stores
scraper *metadata* (the JSON and raw pages that we want in git). Documents
are large binaries with very different storage needs: the metadata for a
meeting is a few KB of text that belongs in version control, while the
agenda pack behind it can be hundreds of megabytes of PDFs that probably
belongs in an object store.

Keeping the two apart means the metadata can be committed to git exactly
like councillors data, while the documents it references are addressed by
key and can live wherever suits the deployment. The metadata records the
content hash and the storage key/URL, so the two halves stay linked.
"""

from __future__ import annotations

import abc
import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class StoredDocument:
    """
    The result of storing one document, recorded in the meeting metadata so
    the file can be found again (and checked for changes) later.
    """

    #: Backend-scoped identifier for this document, e.g. a relative path or
    #: an object store key.
    key: str

    #: Where the document can be retrieved from right now. A ``file://`` URL
    #: for local storage, an ``https://`` or ``s3://`` URL for an object
    #: store. Deliberately *not* recorded in metadata - see ``as_dict()``.
    url: str

    #: ``sha256:<hexdigest>`` of the file contents.
    content_hash: str

    #: Size of the stored file in bytes.
    content_length: int

    #: Name of the backend that stored it, e.g. ``"local"``.
    backend: str

    def as_dict(self) -> dict:
        """
        What gets recorded in the scraped metadata.

        The key and backend name are stored, but the resolved URL is not:
        it is specific to wherever the scrape happened to run. For local
        storage that would bake a developer's home directory into metadata
        destined for git, so the same document would produce a different
        value for every person who scraped it. The key is the document's
        path within its backend, and the backend turns that back into a
        URL via ``url_for()`` whenever one is needed.
        """
        return {
            "storage_key": self.key,
            "storage_backend": self.backend,
            "content_hash": self.content_hash,
            "content_length": self.content_length,
        }


def hash_content(content: bytes) -> str:
    """Return the ``sha256:<hexdigest>`` hash recorded against a document."""
    return "sha256:{}".format(hashlib.sha256(content).hexdigest())


class BaseDocumentStorage(abc.ABC):
    """
    Abstract base class for document storage backends.

    Unlike :class:`lgsf.storage.backends.base.BaseStorage`, document storage
    is not session based. Documents are immutable once written: a given key
    either holds the file or it doesn't, and there is no batch to commit or
    roll back. That makes ``exists()`` a cheap, reliable way for a scraper to
    skip work it has already done.

    Each instance is bound to a single council, mirroring the metadata
    backends.
    """

    #: Short name recorded in metadata as ``storage_backend``.
    name: str = ""

    def __init__(self, council_code: str):
        if not council_code or not council_code.strip():
            raise ValueError("council_code cannot be empty")
        self.council_code = council_code.strip()

    @abc.abstractmethod
    def exists(self, key: str) -> bool:
        """Return True if a document is already stored under ``key``."""
        ...

    @abc.abstractmethod
    def write(self, key: str, content: bytes) -> StoredDocument:
        """
        Store ``content`` under ``key``, overwriting anything already there.

        Returns a :class:`StoredDocument` describing where it went.
        """
        ...

    @abc.abstractmethod
    def read(self, key: str) -> bytes:
        """
        Return the stored document's contents.

        Raises FileNotFoundError if nothing is stored under ``key``.
        """
        ...

    @abc.abstractmethod
    def url_for(self, key: str) -> str:
        """
        Return the retrieval URL for ``key``, whether or not it exists.

        Resolved on demand rather than stored, so that metadata stays
        portable between machines and deployments.
        """
        ...

    def describe(self, key: str) -> StoredDocument | None:
        """
        Return a :class:`StoredDocument` for an already-stored document, or
        None if nothing is stored under ``key``.

        Used to re-record metadata for a document that was skipped because we
        already had it, so a skipped document's JSON looks identical to a
        freshly downloaded one's.
        """
        if not self.exists(key):
            return None
        content = self.read(key)
        return StoredDocument(
            key=key,
            url=self.url_for(key),
            content_hash=hash_content(content),
            content_length=len(content),
            backend=self.name,
        )
