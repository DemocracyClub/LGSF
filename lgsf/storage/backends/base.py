import abc
import contextlib
from pathlib import Path
from typing import Iterator, Literal, Union, Optional

class StorageSession(abc.ABC):
    """Operations that are only valid within a session."""

    @abc.abstractmethod
    def write(self, filename: Path, content: str) -> None:
        """Stage a UTF-8 text file for this session."""
        ...

    @abc.abstractmethod
    def write_bytes(self, filename: Path, content: bytes) -> None:
        """Stage a binary file for this session."""
        ...

    @abc.abstractmethod
    def touch(self, filename: Path) -> None:
        """Stage an empty file for this session."""
        ...

    @abc.abstractmethod
    def open(self, filename: Path, mode: Literal["r", "rb"] = "r") -> Union[str, bytes]:
        """Read a file view for this session (prefers staged content)."""
        ...


class BaseStorage(abc.ABC):
    """
    Pluggable storage that *always* requires a session for any mutating or read-consistent ops.
    """

    # ---- Session lifecycle ----
    def start_session(self, council_code: str, **kwargs) -> StorageSession:
        return self._start_session(council_code, **kwargs)

    @abc.abstractmethod
    def _start_session(self, council_code, **kwargs) -> StorageSession: ...

    def end_session(self, session: StorageSession, commit_message: str, **kwargs):
        return self._end_session(session, commit_message, **kwargs)

    @abc.abstractmethod
    def _end_session(self, session: StorageSession, commit_message: str, **kwargs): ...

    # Optional: let backends clean up on exceptions
    def _reset_session_state(self, session: Optional[StorageSession]) -> None:
        pass

    @contextlib.contextmanager
    def session(self, commit_message: str, **kwargs) -> Iterator[StorageSession]:
        """
        Usage:
            with storage.session("My commit", branch="main") as s:
                s.write(Path("a.txt"), "hello")
        """
        sess = self.start_session(**kwargs)
        try:
            yield sess
            self.end_session(sess, commit_message, **kwargs)
        except Exception:
            self._reset_session_state(sess)
            raise
