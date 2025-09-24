
from typing import Optional, Type
from lgsf.storage.backends.base import BaseStorage
from lgsf.storage.backends.local import LocalFilesystemStorage


def get_storage_backend(council_code: str, backend_type: Optional[str] = None) -> BaseStorage:
    """
    Get a storage backend instance for a specific council.

    Args:
        council_code: Council identifier that this storage instance will serve.
                     Must be non-empty and contain only safe characters.
        backend_type: The type of backend to create. Defaults to 'local'.
                     Currently supported: 'local'

    Returns:
        An instance of the requested storage backend tied to the specified council.

    Raises:
        ValueError: If the backend_type is not supported or council_code is invalid.
    """
    if backend_type is None:
        backend_type = "local"

    backend_type = backend_type.lower()

    if backend_type == "local":
        return LocalFilesystemStorage(council_code=council_code)
    else:
        raise ValueError(f"Unsupported storage backend: {backend_type}")


def get_available_backends() -> list[str]:
    """Return a list of available storage backend types."""
    return ["local"]


if __name__ == "__main__":
    print(f"Test backend: {get_storage_backend('test-council')}")
    print(f"Available backends: {get_available_backends()}")
