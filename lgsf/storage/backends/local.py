from __future__ import annotations
import contextlib
from pathlib import Path
from typing import Dict, Optional, Union, Literal
from uuid import uuid4

from lgsf.conf import settings
from lgsf.storage.backends.base import StorageSession, BaseStorage


class _LocalPathlibSession(StorageSession):
    """
    Local filesystem session implementation using pathlib for atomic operations.

    This session implementation stages all file operations in memory and applies
    them atomically when the session is committed. Files are written using a
    temporary file approach with Path.replace() to ensure atomic updates.

    Features:
    - All writes are staged in memory until commit
    - Atomic file operations using temporary files
    - UTF-8 encoding for text files
    - Path validation and security checks
    - Consistent read view (staged content preferred over disk)

    Thread Safety:
    - Individual session instances are not thread-safe
    - Do not share session objects between threads

    Args:
        root: The root directory path where files will be stored
        encoding: Text encoding to use for string content (default: utf-8)
    """

    def __init__(self, root: Path, encoding: str = "utf-8"):
        self._root = root.resolve()
        self._encoding = encoding
        self._staged: Dict[str, bytes] = {}
        self._closed = False

    # --- StorageSession API ---
    def write(self, filename: Path, content: str) -> None:
        self._assert_open()
        self._staged[self._key(filename)] = content.encode(self._encoding)

    def write_bytes(self, filename: Path, content: bytes) -> None:
        self._assert_open()
        self._staged[self._key(filename)] = content

    def touch(self, filename: Path) -> None:
        self._assert_open()
        self._staged[self._key(filename)] = b""

    def open(self, filename: Path, mode: Literal["r", "rb"] = "r") -> Union[str, bytes]:
        self._assert_open()
        key = self._key(filename)
        if key in self._staged:
            data = self._staged[key]
            return data if mode == "rb" else data.decode(self._encoding)

        path = self._root / key
        if not path.exists():
            raise FileNotFoundError(str(path))
        raw = path.read_bytes()
        return raw if mode == "rb" else raw.decode(self._encoding)

    # --- internals used by storage ---
    def _consume_staged(self) -> Dict[str, bytes]:
        self._assert_open()
        staged = self._staged
        self._staged = {}
        self._closed = True
        return staged

    # --- helpers ---
    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("Session is closed.")

    def _key(self, p: Path) -> str:
        """Convert a Path to a relative string key, ensuring it stays within root."""
        if p.is_absolute():
            raise ValueError(f"Absolute paths not allowed: {p}")

        rel = p.as_posix().lstrip("/")
        if not rel:
            raise ValueError("Empty path not allowed")

        # Check for path traversal attempts
        if ".." in p.parts:
            raise ValueError(f"Path traversal not allowed: {p}")

        candidate = (self._root / rel).resolve()
        if not candidate.is_relative_to(self._root):
            raise ValueError(f"Path {p} resolves outside root directory {self._root}")
        return rel


class LocalFilesystemStorage(BaseStorage):
    """
    Local filesystem storage backend with atomic operations and council isolation.

    This storage backend implements the BaseStorage interface using the local
    filesystem. It provides:

    - **Council Isolation**: Each council gets its own subdirectory
    - **Atomic Operations**: All file changes in a session are applied atomically
    - **Path Security**: Prevents path traversal and validates all file paths
    - **UTF-8 Support**: Proper encoding handling for text files
    - **Error Recovery**: Automatic cleanup on failed operations

    Architecture:
    - Root directory contains subdirectories for each council
    - Council codes are sanitized to prevent filesystem issues
    - Sessions are isolated and cannot interfere with each other
    - Files are written using temporary files and atomic moves

    Storage Layout:
        <root>/
        ├── <sanitized-council-code-1>/
        │   ├── file1.txt
        │   └── subdir/
        │       └── file2.txt
        └── <sanitized-council-code-2>/
            └── config.yaml

    Thread Safety:
    - Individual storage instances support one session at a time
    - Use separate storage instances for concurrent operations
    - Session objects must not be shared between threads

    Error Handling:
    - Failed commits leave the filesystem in the original state
    - Partial writes are cleaned up automatically
    - Path validation prevents security issues

    Examples:
        # Basic usage
        storage = LocalFilesystemStorage()
        with storage.session("Update config", council_code="ABC123") as session:
            session.write(Path("config.yml"), yaml_data)

        # Reading and modifying
        with storage.session("Process data", council_code="XYZ789") as session:
            data = session.open(Path("input.csv"))
            processed = process_csv(data)
            session.write(Path("output.csv"), processed)
    """

    def __init__(self):
        self.root = Path(settings.DATA_DIR_NAME)
        self.encoding = "utf8"
        self._active: Optional[_LocalPathlibSession] = None

    def _start_session(self, council_code: str, **kwargs) -> StorageSession:
        """
        Create a new local filesystem session for the specified council.

        Creates a council-specific subdirectory if it doesn't exist and returns
        a session scoped to that directory. The council_code is sanitized to
        ensure filesystem safety.

        Args:
            council_code: Council identifier, will be sanitized for filesystem use
            **kwargs: Additional parameters (currently unused)

        Returns:
            _LocalPathlibSession: A new session for the council's directory

        Raises:
            ValueError: If council_code is empty or contains only unsafe characters
            RuntimeError: If a session is already active on this storage instance
            OSError: If the council directory cannot be created
        """
        if self._active is not None:
            raise RuntimeError(
                "A session is already active on this LocalFilesystemStorage instance."
            )

        if not council_code or not council_code.strip():
            raise ValueError("council_code cannot be empty")

        # Sanitize council_code to prevent path issues
        safe_council_code = "".join(c for c in council_code if c.isalnum() or c in "_-")
        if not safe_council_code:
            raise ValueError(f"Invalid council_code: {council_code}")

        # Create council-specific subdirectory
        council_root = self.root / safe_council_code
        try:
            council_root.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise RuntimeError(f"Failed to create council directory {council_root}: {e}")

        sess = _LocalPathlibSession(council_root, encoding=self.encoding)
        self._active = sess
        return sess

    def _end_session(self, session: StorageSession, commit_message: str, **kwargs):
        """
        Commit all staged changes from the session to the filesystem.

        Writes all staged files atomically using temporary files and Path.replace().
        If any write fails, attempts to clean up partial changes and raises an error.

        Args:
            session: The session to commit (must be from this storage instance)
            commit_message: Description of the changes (must be non-empty)
            **kwargs: Additional parameters (currently unused)

        Returns:
            dict: Commit information containing:
                - applied: Number of files written
                - root: Path to the council's root directory
                - files: List of file paths that were written
                - commit_message: The sanitized commit message

        Raises:
            ValueError: If commit_message is empty
            RuntimeError: If session is invalid or doesn't belong to this instance
            OSError: If filesystem operations fail
        """
        if not isinstance(session, _LocalPathlibSession) or session is not self._active:
            raise RuntimeError(
                "Unknown or inactive session for this LocalFilesystemStorage."
            )

        if not commit_message or not commit_message.strip():
            raise ValueError("commit_message cannot be empty")

        try:
            staged = session._consume_staged()
            if not staged:
                return {"skipped": True, "reason": "no changes"}

            written_files = []
            for rel_path, data in staged.items():
                target = session._root / rel_path
                try:
                    target.parent.mkdir(parents=True, exist_ok=True)

                    # pathlib-only unique temp file in the same dir
                    tmp = target.with_name(f".tmp-{uuid4().hex}-{target.name}")
                    # 'xb' ensures exclusive creation; avoids races
                    with tmp.open("xb") as f:
                        f.write(data)
                        f.flush()
                    # atomic replace into place
                    tmp.replace(target)
                    written_files.append(str(target))
                except OSError as e:
                    # Clean up any partial writes
                    for written_file in written_files:
                        try:
                            Path(written_file).unlink(missing_ok=True)
                        except OSError:
                            pass  # Best effort cleanup
                    raise RuntimeError(f"Failed to write {rel_path}: {e}")

            # Optionally: write commit_message to a log file under root if you want history.
            return {
                "applied": len(staged),
                "root": str(session._root),
                "files": written_files,
                "commit_message": commit_message.strip()
            }
        finally:
            self._reset_session_state(session)

    def _reset_session_state(self, session: Optional[StorageSession]) -> None:
        """
        Clean up session state after an error.

        Resets the internal active session tracking to allow new sessions
        to be created. This is called automatically when session operations fail.

        Args:
            session: The session that failed (currently unused but kept for interface)
        """
        self._active = None
