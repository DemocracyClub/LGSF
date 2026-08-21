"""Document storage backends and the factory used to select one."""

import os
from typing import Optional

from lgsf.storage.documents.base import (
    BaseDocumentStorage,
    StoredDocument,
    hash_content,
)

__all__ = [
    "BaseDocumentStorage",
    "StoredDocument",
    "hash_content",
    "get_document_storage_backend",
    "get_available_document_backends",
]


def detect_document_storage_backend_from_environment(options: dict) -> str:
    """
    Work out which document backend to use, in priority order: an explicit
    option, then the LGSF_DOCUMENT_STORAGE_BACKEND environment variable,
    then local storage.

    Note this is deliberately independent of the metadata storage backend:
    a Lambda run will typically write metadata to GitHub while putting the
    documents themselves in an object store.
    """
    if options and "document_storage_backend" in options:
        return options["document_storage_backend"]

    backend_from_env = os.environ.get("LGSF_DOCUMENT_STORAGE_BACKEND")
    if backend_from_env:
        return backend_from_env.lower()

    return "local"


def get_document_storage_backend(
    council_code: str,
    backend_type: Optional[str] = None,
    options: Optional[dict] = None,
    **kwargs,
) -> BaseDocumentStorage:
    """
    Get a document storage backend instance for a specific council.

    Args:
        council_code: Council identifier this storage instance will serve.
        backend_type: Backend to create. If None, detected from options and
                     environment.
        options: Scraper options dictionary, used for detection.
        **kwargs: Additional backend-specific parameters.

    Raises:
        ValueError: If the backend_type is not supported.
    """
    if backend_type is None:
        backend_type = detect_document_storage_backend_from_environment(options or {})

    backend_type = backend_type.lower()

    if backend_type == "local":
        from lgsf.storage.documents.local import LocalDocumentStorage

        return LocalDocumentStorage(council_code=council_code)

    raise ValueError(f"Unsupported document storage backend: {backend_type}")


def get_available_document_backends() -> list[str]:
    """Return a list of available document storage backend types."""
    return ["local"]
