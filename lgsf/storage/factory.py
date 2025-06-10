import os
from .base import StorageConfig
from .filesystem import FileSystemStorage
from .codecommit import CodeCommitStorage
from .github import GitHubStorage

def get_storage_backend(config: StorageConfig, backend_name: str = None):
    backend = backend_name or os.environ.get("LGSF_STORAGE_BACKEND", "filesystem").lower()
    if backend == "filesystem":
        return FileSystemStorage(config)
    elif backend == "codecommit":
        return CodeCommitStorage(config)
    elif backend == "github":
        return GitHubStorage(config)
    else:
        raise ValueError(f"Unknown storage backend: {backend}") 