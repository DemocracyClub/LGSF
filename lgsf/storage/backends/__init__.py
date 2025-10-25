import os
from typing import Optional

from lgsf.storage.backends.base import BaseStorage


def detect_storage_backend_from_environment(options: dict) -> str:
    """
    Detect the appropriate storage backend based on environment and options.

    Args:
        options: Scraper options dictionary

    Returns:
        str: The backend type to use
    """
    # Check for explicit backend specification
    if "storage_backend" in options:
        return options["storage_backend"]

    # Check environment variables
    backend_from_env = os.environ.get("LGSF_STORAGE_BACKEND")
    if backend_from_env:
        return backend_from_env.lower()

    # Default to local storage
    return "local"


def get_storage_backend(
    council_code: str,
    backend_type: Optional[str] = None,
    options: Optional[dict] = None,
    **kwargs,
) -> BaseStorage:
    """
    Get a storage backend instance for a specific council.

    Args:
        council_code: Council identifier that this storage instance will serve.
                     Must be non-empty and contain only safe characters.
        backend_type: The type of backend to create. If None, will be detected
                     from environment and options.
        options: Scraper options dictionary for backend detection.
        **kwargs: Additional backend-specific parameters:
                 - scraper_object_type: For github backend, the type of scraper data
                 - organization: For github backend, the organization name
                 - github_token: For github backend, the authentication token

    Returns:
        An instance of the requested storage backend tied to the specified council.

    Raises:
        ValueError: If the backend_type is not supported or council_code is invalid.
    """
    if backend_type is None:
        if options:
            backend_type = detect_storage_backend_from_environment(options)
        else:
            backend_type = "local"

    backend_type = backend_type.lower()

    if backend_type == "local":
        from lgsf.storage.backends.local import LocalFilesystemStorage

        return LocalFilesystemStorage(council_code=council_code)
    elif backend_type == "github":
        scraper_object_type = kwargs.get("scraper_object_type", "Data")
        from lgsf.storage.backends.github import GitHubStorage

        return GitHubStorage(
            council_code=council_code,
            scraper_object_type=scraper_object_type,
            organization=kwargs.get("organization"),
            github_token=kwargs.get("github_token"),
        )
    else:
        raise ValueError(f"Unsupported storage backend: {backend_type}")


def get_available_backends() -> list[str]:
    """Return a list of available storage backend types."""
    return ["local", "github"]


if __name__ == "__main__":
    print(f"Test backend: {get_storage_backend('test-council')}")
    print(f"Available backends: {get_available_backends()}")
