
from typing import Optional, Type
from lgsf.storage.backends.base import BaseStorage
from lgsf.storage.backends.local import LocalFilesystemStorage


def get_storage_backend(backend_type: Optional[str] = None) -> BaseStorage:
    """
    Get a storage backend instance.

    Args:
        backend_type: The type of backend to create. Defaults to 'local'.
                     Currently supported: 'local'

    Returns:
        An instance of the requested storage backend.

    Raises:
        ValueError: If the backend_type is not supported.
    """
    if backend_type is None:
        backend_type = "local"

    backend_type = backend_type.lower()

    if backend_type == "local":
        return LocalFilesystemStorage()
    else:
        raise ValueError(f"Unsupported storage backend: {backend_type}")


def get_available_backends() -> list[str]:
    """Return a list of available storage backend types."""
    return ["local"]


if __name__ == "__main__":
    print(f"Default backend: {get_storage_backend()}")
    print(f"Available backends: {get_available_backends()}")
