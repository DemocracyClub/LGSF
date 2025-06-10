import abc
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

@dataclass
class StorageConfig:
    """Configuration for storage backends"""
    base_path: str
    council_id: str
    verbose: bool = False

class StorageBackend(metaclass=abc.ABCMeta):
    """Base class for all storage backends"""
    
    def __init__(self, config: StorageConfig):
        self.config = config

    @abc.abstractmethod
    def save(self, path: str, content: Any) -> None:
        """Save content to the given path"""
        pass

    @abc.abstractmethod
    def load(self, path: str) -> Any:
        """Load content from the given path"""
        pass

    @abc.abstractmethod
    def delete(self, path: str) -> None:
        """Delete content at the given path"""
        pass

    @abc.abstractmethod
    def exists(self, path: str) -> bool:
        """Check if content exists at the given path"""
        pass

class FileSystemStorage(StorageBackend):
    """Storage backend for local file system"""
    pass

class CodeCommitStorage(StorageBackend):
    """Storage backend for AWS CodeCommit"""
    pass

class CouncillorStorage(StorageBackend):
    """Storage backend for councillor data"""
    pass 