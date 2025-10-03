import abc
import contextlib
from pathlib import Path
from typing import Iterator, Literal, Optional, Union


class StorageSession(abc.ABC):
    """
    A storage session provides isolated, transactional operations on files.

    StorageSession represents a single "transaction" or "batch" of file operations
    that are staged in memory and only committed to persistent storage when the
    session ends successfully. This provides:

    - **Atomicity**: All operations in a session succeed or fail together
    - **Isolation**: Changes are not visible until the session is committed
    - **Consistency**: The storage remains in a valid state even if operations fail

    Sessions are designed to be used within a context manager:

    Examples:
        Basic usage:
            with storage.session("My changes") as session:
                session.write(Path("config.txt"), "new config")
                session.write_bytes(Path("data.bin"), binary_data)
                session.touch(Path("marker.flag"))
                # Files are committed when context exits successfully

        Reading files (prefers staged content over disk):
            with storage.session("Read data") as session:
                content = session.open(Path("existing.txt"))
                session.write(Path("updated.txt"), content + " modified")
                # session.open() would return the modified content

    All file paths should be relative to the storage root and will be scoped
    to the council_code specified when the storage backend was created.
    """

    @abc.abstractmethod
    def write(self, filename: Path, content: str) -> None:
        """
        Stage a UTF-8 text file for writing in this session.

        The file content is staged in memory and will only be written to persistent
        storage when the session is committed. If a file with the same path is
        written multiple times in the same session, the last write wins.

        Args:
            filename: Relative path where the file should be written. Must not be
                     absolute or contain path traversal elements like '../'.
            content: Text content to write. Will be encoded as UTF-8.

        Raises:
            ValueError: If filename is invalid (absolute, empty, contains path traversal)
            RuntimeError: If the session is closed or in an invalid state

        Example:
            session.write(Path("config.yaml"), "database_url: localhost")
            session.write(Path("docs/readme.txt"), "Project documentation")
        """
        ...

    @abc.abstractmethod
    def write_bytes(self, filename: Path, content: bytes) -> None:
        """
        Stage a binary file for writing in this session.

        Similar to write() but for binary content. The file content is staged in
        memory and will only be written to persistent storage when the session
        is committed.

        Args:
            filename: Relative path where the file should be written. Must not be
                     absolute or contain path traversal elements like '../'.
            content: Binary content to write as bytes.

        Raises:
            ValueError: If filename is invalid (absolute, empty, contains path traversal)
            RuntimeError: If the session is closed or in an invalid state

        Example:
            with open("image.png", "rb") as f:
                session.write_bytes(Path("assets/logo.png"), f.read())
        """
        ...

    @abc.abstractmethod
    def touch(self, filename: Path) -> None:
        """
        Stage an empty file for creation in this session.

        Creates a zero-byte file at the specified path. Useful for creating
        marker files, lock files, or placeholder files that will be populated later.

        Args:
            filename: Relative path where the empty file should be created. Must not
                     be absolute or contain path traversal elements like '../'.

        Raises:
            ValueError: If filename is invalid (absolute, empty, contains path traversal)
            RuntimeError: If the session is closed or in an invalid state

        Example:
            session.touch(Path("logs/processing.lock"))
            session.touch(Path("data/placeholder.txt"))
        """
        ...

    @abc.abstractmethod
    def open(self, filename: Path, mode: Literal["r", "rb"] = "r") -> Union[str, bytes]:
        """
        Read a file within this session context.

        This method provides a consistent view of files that respects the session's
        staged changes. If a file has been modified in this session, the staged
        content is returned. Otherwise, the content is read from persistent storage.

        This allows you to read files that you've modified within the same session,
        which is useful for workflows that build upon previous changes.

        Args:
            filename: Relative path to the file to read. Must not be absolute
                     or contain path traversal elements like '../'.
            mode: File reading mode:
                  - "r": Read as UTF-8 text (returns str)
                  - "rb": Read as binary (returns bytes)

        Returns:
            str: File content as text if mode="r"
            bytes: File content as bytes if mode="rb"

        Raises:
            FileNotFoundError: If the file doesn't exist in staged changes or storage
            ValueError: If filename is invalid (absolute, empty, contains path traversal)
            RuntimeError: If the session is closed or in an invalid state
            UnicodeDecodeError: If mode="r" but file contains invalid UTF-8

        Example:
            # Read existing file
            config = session.open(Path("config.yaml"))

            # Modify and re-read in same session
            session.write(Path("data.txt"), "new content")
            content = session.open(Path("data.txt"))  # Returns "new content"

            # Read binary data
            image_data = session.open(Path("logo.png"), mode="rb")
        """
        ...


class BaseStorage(abc.ABC):
    """
    Abstract base class for pluggable storage backends with session-based operations.

    BaseStorage defines the interface for storage systems that require explicit
    sessions for all file operations. Each storage instance is tied to a specific
    council for data isolation and security. This design provides:

    - **Transaction-like behavior**: All operations are batched and atomic
    - **Council isolation**: Each instance is bound to one specific council
    - **Backend flexibility**: Implementations can use local filesystem, git, cloud storage, etc.
    - **Consistency guarantees**: Sessions ensure data integrity

    Architecture:
    - All file operations must occur within a StorageSession
    - Each storage instance serves exactly one council
    - Sessions provide atomic commit/rollback semantics
    - Backends can implement different storage mechanisms (filesystem, git, S3, etc.)
    - Backend-specific operations (like cleanup, merging) are handled within session lifecycle

    Usage Pattern:
        storage = get_storage_backend(council_code="ABC123", options=options)

        # Option 1: Using context manager (recommended)
        with storage.session("Update config") as session:
            session.write(Path("config.yml"), yaml_content)
            session.write(Path("readme.txt"), "Updated docs")
            # Automatically commits on successful exit

        # Option 2: Manual session management
        session = storage.start_session()
        try:
            session.write(Path("data.txt"), content)
            storage.end_session(session, "Manual commit")
        except Exception:
            storage._reset_session_state(session)  # Cleanup
            raise

    Implementation Requirements:
    - Subclasses must implement __init__() to accept and store council_code
    - Subclasses must implement _start_session() and _end_session()
    - File operations within a session must be atomic
    - Backend-specific preparation should happen in _start_session()
    - Backend-specific finalization should happen in _end_session()
    - Optionally implement _reset_session_state() for cleanup on errors

    Thread Safety:
    - Individual storage instances are generally not thread-safe
    - Use separate storage instances per thread if needed
    - Session objects should not be shared across threads
    """

    def __init__(self, council_code: str):
        """
        Initialize storage backend for a specific council.

        Args:
            council_code: Identifier for the council/organization this storage
                         instance will serve. Must be non-empty and contain only
                         safe characters.

        Raises:
            ValueError: If council_code is invalid (empty, unsafe characters)
        """
        if not council_code or not council_code.strip():
            raise ValueError("council_code cannot be empty")

        self.council_code = council_code.strip()

    # ---- Session lifecycle ----
    def start_session(self, **kwargs) -> StorageSession:
        """
        Start a new storage session for this storage instance's council.

        Creates and returns a new StorageSession that can be used to perform
        file operations. All operations within the session are isolated and
        will only be committed when end_session() is called successfully.

        Backend-specific preparation (like cleaning directories or deleting
        existing data) should be handled in the _start_session() implementation.

        Args:
            **kwargs: Additional backend-specific parameters.

        Returns:
            StorageSession: A new session object for performing file operations.

        Raises:
            RuntimeError: If a session is already active for this storage instance
                         (for backends that don't support concurrent sessions)

        Note:
            It's recommended to use the session() context manager instead of
            manually managing start_session/end_session calls.
        """
        return self._start_session(**kwargs)

    @abc.abstractmethod
    def _start_session(self, **kwargs) -> StorageSession:
        """
        Backend-specific session creation logic.

        This method should:
        1. Perform any backend-specific preparation (cleanup, initialization)
        2. Create and return a new session object
        3. Handle any setup that needs to happen before file operations

        Args:
            **kwargs: Backend-specific parameters

        Returns:
            StorageSession: New session ready for file operations
        """
        ...

    def end_session(self, session: StorageSession, commit_message: str, **kwargs):
        """
        Commit and close a storage session.

        Applies all staged changes from the session to persistent storage atomically.
        If any operation fails, the entire session is rolled back and no changes
        are applied.

        Backend-specific finalization (like merging branches or updating logs)
        should be handled in the _end_session() implementation.

        Args:
            session: The StorageSession to commit. Must be a session created by
                    this storage instance's start_session() method.
            commit_message: Descriptive message explaining the changes being committed.
                          Used for logging, audit trails, or version control.
                          Must be non-empty.
            **kwargs: Additional backend-specific parameters for the commit operation.

        Returns:
            dict: Information about the commit operation, typically including:
                 - Number of files changed
                 - Commit identifier or timestamp
                 - Backend-specific metadata

        Raises:
            ValueError: If commit_message is empty or session is invalid
            RuntimeError: If session doesn't belong to this storage instance,
                         or if the commit operation fails
            OSError: If there are filesystem or network errors during commit

        Note:
            After end_session() is called (successfully or not), the session
            should be considered closed and must not be used for further operations.
        """
        return self._end_session(session, commit_message, **kwargs)

    @abc.abstractmethod
    def _end_session(self, session: StorageSession, commit_message: str, **kwargs):
        """
        Backend-specific session commit logic.

        This method should:
        1. Commit all staged changes atomically
        2. Perform any backend-specific finalization
        3. Return information about the commit
        4. Clean up session state

        Args:
            session: Session to commit
            commit_message: Commit message
            **kwargs: Backend-specific parameters

        Returns:
            dict: Commit information
        """
        ...

    def _reset_session_state(self, session: Optional[StorageSession]) -> None:
        """
        Reset session state after an error occurs.

        This method is called when an exception occurs during session operations
        to allow backends to clean up any internal state or temporary resources.
        The default implementation does nothing.

        Args:
            session: The session that encountered an error, or None if no session
                    was successfully created.

        Note:
            This method should be idempotent and should not raise exceptions.
            It's intended for cleanup operations only.
        """
        pass

    @contextlib.contextmanager
    def session(self, commit_message: str, **kwargs) -> Iterator[StorageSession]:
        """
        Context manager for safe session handling with automatic commit/cleanup.

        This is the recommended way to work with storage sessions. It automatically
        handles session lifecycle:
        - Creates a new session on entry (with backend-specific preparation)
        - Commits changes on successful exit (with backend-specific finalization)
        - Performs cleanup on exceptions

        Args:
            commit_message: Descriptive message for the changes. Will be used when
                          the session is committed on successful exit.
            **kwargs: Additional backend-specific parameters passed to start_session().

        Yields:
            StorageSession: The active session for performing file operations.

        Raises:
            ValueError: If commit_message is empty or other validation fails
            RuntimeError: If session creation or commit fails
            OSError: If there are filesystem or network errors

        Examples:
            # Basic usage
            storage = get_storage_backend(council_code="ABC123", options=options)
            with storage.session("Update config") as session:
                session.write(Path("config.yml"), config_data)
                session.touch(Path("updated.flag"))

            # Exception handling (cleanup happens automatically)
            try:
                with storage.session("Risky operation") as session:
                    session.write(Path("data.txt"), risky_operation())
            except Exception as e:
                print(f"Operation failed, changes rolled back: {e}")
        """
        sess = self.start_session(**kwargs)
        try:
            yield sess
            self.end_session(sess, commit_message, **kwargs)
        except Exception:
            self._reset_session_state(sess)
            raise
