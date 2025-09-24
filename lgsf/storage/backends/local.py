from __future__ import annotations
import contextlib
from pathlib import Path
from typing import Dict, Optional, Union, Literal
from uuid import uuid4

from lgsf.conf import settings
from lgsf.storage.backends.base import StorageSession, BaseStorage


class _LocalPathlibSession(StorageSession):
    """
    Local session using only pathlib. Writes are applied on commit by
    writing a temp file in the target directory and Path.replace() atomically.
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
    Always-session local storage using pathlib only.
    On commit, each staged file is written to a temp file next to the target,
    then atomically moved into place via Path.replace().
    """

    def __init__(self):
        self.root = Path(settings.DATA_DIR_NAME)
        self.encoding = "utf8"
        self._active: Optional[_LocalPathlibSession] = None

    def _start_session(self, council_code: str, **kwargs) -> StorageSession:
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
        self._active = None
