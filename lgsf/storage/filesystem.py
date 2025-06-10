import json
import os
from typing import Any, Optional

from .base import FileSystemStorage, StorageConfig

class FileSystemStorage(FileSystemStorage):
    """Storage backend for local file system operations"""

    def __init__(self, config: StorageConfig):
        super().__init__(config)
        self._ensure_base_path()

    def _ensure_base_path(self) -> None:
        """Ensure the base path exists"""
        os.makedirs(self.config.base_path, exist_ok=True)

    def _get_full_path(self, path: str) -> str:
        """Get the full path by joining with base path"""
        return os.path.join(self.config.base_path, path)

    def save(self, path: str, content: Any) -> None:
        """Save content to the given path"""
        full_path = self._get_full_path(path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        if isinstance(content, (dict, list)):
            with open(full_path, 'w') as f:
                json.dump(content, f, indent=4)
        else:
            with open(full_path, 'w') as f:
                f.write(str(content))

    def load(self, path: str) -> Any:
        """Load content from the given path"""
        full_path = self._get_full_path(path)
        if not os.path.exists(full_path):
            return None

        with open(full_path, 'r') as f:
            if path.endswith('.json'):
                return json.load(f)
            return f.read()

    def delete(self, path: str) -> None:
        """Delete content at the given path"""
        full_path = self._get_full_path(path)
        if os.path.exists(full_path):
            os.remove(full_path)

    def exists(self, path: str) -> bool:
        """Check if content exists at the given path"""
        return os.path.exists(self._get_full_path(path))

    def clean_directory(self, path: str) -> None:
        """Clean all contents of a directory"""
        full_path = self._get_full_path(path)
        if os.path.exists(full_path):
            for item in os.listdir(full_path):
                item_path = os.path.join(full_path, item)
                if os.path.isfile(item_path):
                    os.remove(item_path)
                elif os.path.isdir(item_path):
                    import shutil
                    shutil.rmtree(item_path) 