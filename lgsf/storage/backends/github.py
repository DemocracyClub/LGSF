from __future__ import annotations

import base64
import logging
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Callable, override

import jwt
import requests

from lgsf.storage.backends.base import BaseStorage, StorageMode, StorageSession

logger = logging.getLogger(__name__)


class GitHubRateLimitError(Exception):
    """
    Raised when a GitHub API call hit a rate limit.

    Not retried: the primary limit resets on the hour, and waiting that out
    inside a Lambda would only time out. The worker reports it as a 429 so
    the run shows as rate limited rather than failed.
    """

    def __init__(self, message: str, reset_time: int | None = None):
        super().__init__(message)
        #: Unix time the limit resets, from X-RateLimit-Reset, when known.
        self.reset_time = reset_time


#: Responses worth trying again: GitHub's own trouble, not ours.
RETRYABLE_STATUSES = frozenset({500, 502, 503, 504})

#: A secondary rate limit this short is waited out once; longer is reported.
MAX_RETRY_AFTER_WAIT = 60


def github_api_request(
    http: Any,
    method: str,
    url: str,
    *,
    attempts: int = 4,
    sleep: Callable[[float], None] = time.sleep,
    **kwargs: Any,
) -> requests.Response:
    """
    One GitHub API call, with the handling every call needs.

    ``http`` is anything with a ``request(method, url, **kwargs)`` method:
    a requests.Session, or the requests module itself.

    - A connection error or a 5xx is retried, up to ``attempts`` times in
      all, waiting 1, 2, 4... seconds between tries. The last response is
      returned as it came, for the caller to raise on.
    - A rate limit (403 or 429 with X-RateLimit-Remaining at 0 or a
      Retry-After header) raises GitHubRateLimitError straight away, except
      a secondary limit asking for under MAX_RETRY_AFTER_WAIT seconds, which
      is waited out once.
    - Any other response is returned for the caller to interpret: a 404 on
      an existence check is an answer, not an error.
    """
    kwargs.setdefault("timeout", 30)
    delay = 1.0
    waited_for_secondary_limit = False
    attempt = 0
    while True:
        attempt += 1
        try:
            response = http.request(method, url, **kwargs)
        except requests.exceptions.RequestException as e:
            if attempt >= attempts:
                raise
            logger.warning(f"{method} {url} failed ({e}); retrying in {delay:.0f}s")
            sleep(delay)
            delay *= 2
            continue

        if response.status_code in (403, 429) and _is_rate_limited(response):
            retry_after = _retry_after(response)
            if (
                retry_after is not None
                and retry_after <= MAX_RETRY_AFTER_WAIT
                and not waited_for_secondary_limit
            ):
                logger.warning(
                    f"{method} {url}: secondary rate limit, waiting {retry_after}s"
                )
                waited_for_secondary_limit = True
                sleep(retry_after)
                continue
            raise GitHubRateLimitError(
                f"GitHub rate limit hit on {method} {url}",
                reset_time=_reset_time(response),
            )

        if response.status_code in RETRYABLE_STATUSES and attempt < attempts:
            logger.warning(
                f"{method} {url} returned {response.status_code}; retrying in {delay:.0f}s"
            )
            sleep(delay)
            delay *= 2
            continue

        return response


def _is_rate_limited(response: requests.Response) -> bool:
    headers = response.headers
    if headers.get("X-RateLimit-Remaining") == "0" or "Retry-After" in headers:
        return True
    return "rate limit" in (response.text or "").lower()


def _retry_after(response: requests.Response) -> float | None:
    value = response.headers.get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def _reset_time(response: requests.Response) -> int | None:
    value = response.headers.get("X-RateLimit-Reset")
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


class GitHubAppAuthenticator:
    """Handles GitHub App authentication and token generation."""

    def __init__(
        self,
        app_id: str | None = None,
        installation_id: str | None = None,
        private_key: str | None = None,
    ):
        """
        Initialize GitHub App authenticator.

        Args:
            app_id: GitHub App ID
            installation_id: GitHub App Installation ID
            private_key: GitHub App private key (PEM format)
        """
        self.app_id = app_id or os.environ.get("GITHUB_APP_ID", "")
        self.installation_id = installation_id or os.environ.get(
            "GITHUB_APP_INSTALLATION_ID", ""
        )
        self.private_key = private_key or os.environ.get("GITHUB_APP_PRIVATE_KEY", "")
        #: What makes the HTTP calls: the requests module, or a stand-in in tests.
        self._http: Any = requests

        self._installation_token: str | None = None
        self._token_expires_at: float | None = None

    def is_configured(self) -> bool:
        """Check if GitHub App credentials are configured."""
        return bool(self.app_id and self.installation_id and self.private_key)

    def _generate_jwt(self) -> str:
        """Generate a JWT for GitHub App authentication."""
        now = int(time.time())
        payload = {
            "iat": now,
            "exp": now + 600,  # JWT expires in 10 minutes
            "iss": self.app_id,
        }

        return jwt.encode(payload, self.private_key, algorithm="RS256")

    def _request_installation_token(self) -> tuple[str, float]:
        """Request an installation access token from GitHub."""
        jwt_token = self._generate_jwt()

        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "LGSF-GitHub-Storage/3.0",
        }

        response = github_api_request(
            self._http,
            "POST",
            f"https://api.github.com/app/installations/{self.installation_id}/access_tokens",
            headers=headers,
        )
        response.raise_for_status()

        data = response.json()
        token = data["token"]
        # Token expires in 1 hour, but we'll refresh 5 minutes early to be safe
        expires_at = time.time() + 3300

        return token, expires_at

    def get_token(self) -> str:
        """
        Get a valid installation token, refreshing if necessary.

        Returns:
            A valid GitHub App installation token
        """
        if not self.is_configured():
            raise ValueError(
                "GitHub App not configured. Set GITHUB_APP_ID, "
                "GITHUB_APP_INSTALLATION_ID, and GITHUB_APP_PRIVATE_KEY"
            )

        # Check if we have a valid token
        if self._installation_token and self._token_expires_at:
            if time.time() < self._token_expires_at:
                return self._installation_token

        # Request new token
        logger.info("Requesting new GitHub App installation token")
        self._installation_token, self._token_expires_at = (
            self._request_installation_token()
        )

        return self._installation_token


class _GitHubSession(StorageSession):
    """
    GitHub session implementation using local Git operations.

    This session implementation stages all file operations in memory and uses
    Git CLI commands to perform all operations locally, minimizing API calls.
    """

    def __init__(
        self,
        organization: str,
        github_token: str,
        council_code: str,
        scraper_object_type: str = "Data",
        run_id: str | None = None,
        storage_mode: StorageMode = StorageMode.REPLACE,
    ):
        self.organization: str = organization
        self.github_token: str = github_token
        self.council_code: str = council_code
        self.scraper_object_type: str = scraper_object_type
        self.storage_mode: str = storage_mode
        self.run_id: str = run_id or self._generate_run_id()

        # Repository configuration
        self.owner: str = organization
        self.repo: str = council_code.upper()
        self.repository_url: str = f"https://github.com/{self.owner}/{self.repo}"

        # Session state
        self._staged: dict[str, bytes] = {}
        self._closed: bool = False

        # Local Git repository path (in Lambda's /tmp)
        self.local_repo_path: str = f"/tmp/lgsf-repos/{self.council_code}"

        # Remote URL with authentication
        self.remote_url: str = (
            f"https://x-access-token:{self.github_token}@github.com/"
            f"{self.owner}/{self.repo}.git"
        )

        self._default_branch: str = "main"

        # GitHub API session (only for creating a repository that doesn't exist)
        self.session: requests.Session = self._create_api_session()

        # CRITICAL: Check that Git is available immediately
        # This will fail the Lambda early if Git layer is missing
        self._check_git_available()

        # Configure Git identity for commits
        self._configure_git_identity()

    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        return f"{str(uuid.uuid4())[:8]}-{int(time.time() * 1000) % 10000:04d}"

    #: time.sleep, replaceable in tests so retries don't wait.
    _sleep: Callable[[float], None] = staticmethod(time.sleep)

    def _api(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """A GitHub API call with retries and rate-limit handling; see github_api_request."""
        return github_api_request(
            self.session, method, url, sleep=self._sleep, **kwargs
        )

    def _create_api_session(self) -> requests.Session:
        """Create and configure the GitHub API session."""
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"token {self.github_token}",
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "LGSF-GitHub-Storage/3.0",
            }
        )
        return session

    @property
    def _type_dir(self) -> str:
        """
        Where this session's files live in the local clone: ``<repo>/<Type>/``.

        Each data type (Councillors, Minutes, Decisions...) has its own
        directory in a council's repository, mirroring the local backend's
        ``data/<COUNCIL>/<Type>/``. Staged paths such as ``json/x.json`` are
        relative to this.
        """
        return os.path.join(self.local_repo_path, self.scraper_object_type)

    @property
    def api_base_url(self) -> str:
        """Returns GitHub API base URL for this repository"""
        return f"https://api.github.com/repos/{self.owner}/{self.repo}"

    def _configure_git_identity(self) -> None:
        """
        Configure Git user identity for commits.

        Git requires user.name and user.email to be set before making commits.
        In Lambda, we can't use --global (no HOME), so we set environment variables
        that Git will use for all operations.
        """
        try:
            # Set environment variables for Git identity
            # These work even without a HOME directory
            os.environ["GIT_AUTHOR_NAME"] = "LGSF Bot"
            os.environ["GIT_AUTHOR_EMAIL"] = "lgsf-bot@democracyclub.org.uk"
            os.environ["GIT_COMMITTER_NAME"] = "LGSF Bot"
            os.environ["GIT_COMMITTER_EMAIL"] = "lgsf-bot@democracyclub.org.uk"

            logger.info(
                "Git identity configured via environment variables: LGSF Bot <lgsf-bot@democracyclub.org.uk>"
            )
        except Exception as e:
            logger.error(f"Failed to configure git identity: {e}")
            raise RuntimeError(
                f"CRITICAL: Failed to configure git user identity: {e}"
            ) from e

    def _check_git_available(self) -> None:
        """
        Check if git command is available.

        This is called during session initialization to fail fast if Git is not available.
        Without Git, the backend cannot function, so we raise immediately.
        """
        try:
            result = subprocess.run(
                ["git", "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                logger.info(f"Git is available: {result.stdout.strip()}")
            else:
                logger.error(
                    f"Git command failed with exit code {result.returncode}. "
                    f"Stdout: {result.stdout}, Stderr: {result.stderr}"
                )
                raise RuntimeError(
                    f"CRITICAL: Git command not working properly. Exit code: {result.returncode}. "
                    "The Git Lambda layer may not be properly configured."
                )
        except FileNotFoundError as e:
            logger.error(
                "Git command not found in PATH. The Git Lambda layer is not attached or not working."
            )
            raise RuntimeError(
                "CRITICAL: Git command not found. The Git Lambda layer is not attached to this Lambda function. "
                "Check CDK stack configuration and ensure the Git layer is properly deployed. "
                "Expected layer ARN: arn:aws:lambda:{region}:553035198032:layer:git-lambda2:8"
            ) from e
        except subprocess.TimeoutExpired as e:
            logger.error("Git version check timed out after 5 seconds")
            raise RuntimeError(
                "CRITICAL: Git version check timed out. Git may be installed but not functioning properly."
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error checking git availability: {e}")
            raise RuntimeError(
                f"CRITICAL: Failed to verify git availability: {e}"
            ) from e

    def _run_git_command(
        self,
        args: list[str],
        check: bool = True,
        capture_output: bool = True,
        cwd: str | None = None,
    ) -> subprocess.CompletedProcess:
        """
        Run a git command with proper error handling.

        Args:
            args: Git command arguments (e.g., ['status', '--short'])
            check: Whether to raise on non-zero exit code
            capture_output: Whether to capture stdout/stderr
            cwd: Working directory (defaults to local_repo_path)

        Returns:
            CompletedProcess object with stdout, stderr, and returncode
        """
        cmd = ["git"] + args
        working_dir = cwd or self.local_repo_path

        logger.debug(f"Running git command: {' '.join(cmd)} in {working_dir}")

        # The bot's git, not the developer's: no global or system config,
        # so no commit signing (which on a laptop can block on a passphrase
        # prompt), no hooks, no templates. The Lambda has none of those
        # anyway; this makes a local run behave the same. Identity comes
        # from the environment, see _configure_git_identity, and the
        # remote URL carries its own credentials.
        env = {
            **os.environ,
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
        }

        try:
            result = subprocess.run(
                cmd,
                cwd=working_dir,
                env=env,
                check=check,
                capture_output=capture_output,
                text=True,
                timeout=300,  # 5 minute timeout for git operations
            )
            return result
        except FileNotFoundError:
            raise RuntimeError(
                "Git command not found. The Git Lambda layer may not be properly configured. "
                "Ensure the Git Lambda layer is attached to this function."
            )
        except subprocess.CalledProcessError as e:
            logger.error(
                f"Git command failed: {' '.join(cmd)}\n"
                f"Exit code: {e.returncode}\n"
                f"Stdout: {e.stdout}\n"
                f"Stderr: {e.stderr}"
            )
            raise
        except subprocess.TimeoutExpired:
            logger.error(f"Git command timed out after 300s: {' '.join(cmd)}")
            raise

    def _repo_exists_on_github(self) -> bool:
        """
        Whether the council's repository exists.

        A 404 is the answer "no". Anything else that isn't a 200, once the
        retries are spent, raises: guessing "no" would go on to try to
        create the repository, and a rate limit must reach the handler.
        """
        response = self._api("GET", self.api_base_url)
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def _check_if_repo_is_empty(self) -> bool:
        """Whether the repository exists but has no main branch yet."""
        response = self._api(
            "GET", f"{self.api_base_url}/git/refs/heads/{self._default_branch}"
        )
        if response.status_code == 404:
            logger.info(f"Repository exists but has no {self._default_branch} branch")
            return True
        response.raise_for_status()
        return False

    def _create_repository(self) -> None:
        """Create a new repository on GitHub using API and ensure it has an initial commit."""
        logger.info(f"Creating new repository: {self.owner}/{self.repo}")

        # Create repository without auto_init so we can control the initial commit
        create_data = {
            "name": self.repo,
            "description": f"Data storage for council {self.council_code}",
            "private": True,
            "has_issues": False,
            "has_projects": False,
            "has_wiki": False,
            "auto_init": False,  # We'll create our own initial commit
        }

        response = self._api(
            "POST", f"https://api.github.com/orgs/{self.owner}/repos", json=create_data
        )

        if response.status_code == 201:
            logger.info(f"Repository created successfully: {self.repository_url}")
            # Now create the initial commit via API with custom README
            self._create_initial_commit_via_api()
        elif response.status_code == 422:
            # Repository might already exist
            logger.info(f"Repository might already exist: {self.owner}/{self.repo}")
        else:
            response.raise_for_status()

    def _create_initial_commit_via_api(self) -> None:
        """Create initial commit with README via GitHub API."""
        logger.info(f"Creating initial commit for {self.owner}/{self.repo}")

        # Create README content
        readme_content = (
            f"# Data for {self.council_code}\n\n"
            f"This repository contains scraped data from the "
            f"Local Government Scraper Framework (LGSF).\n"
        )

        # Encode content to base64 for GitHub API
        encoded_content = base64.b64encode(readme_content.encode()).decode()

        # Create file via API (this will create the initial commit)
        file_data = {
            "message": "Initial commit",
            "content": encoded_content,
            "branch": self._default_branch,
        }

        response = self._api(
            "PUT", f"{self.api_base_url}/contents/README.md", json=file_data
        )

        if response.status_code in (200, 201):
            logger.info("Initial commit created successfully")
            # Wait a moment for GitHub to process the commit
            self._sleep(2)
        else:
            logger.error(
                f"Failed to create initial commit: {response.status_code} - {response.text}"
            )
            response.raise_for_status()

    def _initialize_empty_repo(self) -> None:
        """
        Initialize an empty local repository when the remote exists but has no commits.

        This handles the case where a repository was created on GitHub but is empty
        (no initial commit or main branch).
        """
        logger.info(f"Initializing empty repository locally: {self.local_repo_path}")

        # Create directory
        os.makedirs(self.local_repo_path, exist_ok=True)

        # Initialize git repo
        self._run_git_command(["init"], cwd=self.local_repo_path)

        # Set remote
        self._run_git_command(
            ["remote", "add", "origin", self.remote_url],
            cwd=self.local_repo_path,
        )

        # Create initial README with council name
        readme_path = os.path.join(self.local_repo_path, "README.md")
        with open(readme_path, "w") as f:
            f.write(
                f"# Data for {self.council_code}\n\n"
                "This repository contains scraped data from the "
                "Local Government Scraper Framework (LGSF).\n"
            )

        # Stage, commit, and push initial commit
        self._run_git_command(["add", "README.md"], cwd=self.local_repo_path)
        self._run_git_command(
            ["commit", "-m", "Initial commit"],
            cwd=self.local_repo_path,
        )

        # Create main branch and push
        self._run_git_command(
            ["branch", "-M", self._default_branch],
            cwd=self.local_repo_path,
        )
        self._run_git_command(
            ["push", "-u", "origin", self._default_branch],
            cwd=self.local_repo_path,
        )

        logger.info("Empty repository initialized with main branch")

    def _ensure_local_repo(self) -> None:
        """
        Ensure local repository exists and is up to date.

        This will:
        1. Clone the repo if it doesn't exist locally
        2. Pull latest changes if it does exist
        3. Create the repo on GitHub if it doesn't exist there
        4. Initialize empty repos (repos with no commits) before doing anything else
        """
        if not os.path.exists(self.local_repo_path):
            # Need to clone - first ensure repo exists on GitHub
            repo_exists = self._repo_exists_on_github()
            if not repo_exists:
                self._create_repository()
                repo_exists = True

            # Check if repo exists but is empty (no commits)
            if repo_exists:
                is_empty = self._check_if_repo_is_empty()
                if is_empty:
                    logger.info("Repository exists but has no commits, initializing...")
                    self._initialize_empty_repo()
                    return

            # Clone repository (shallow clone for speed)
            logger.info(f"Cloning repository: {self.repository_url}")
            os.makedirs(os.path.dirname(self.local_repo_path), exist_ok=True)

            # Retry clone up to 3 times (in case repo was just created)
            max_retries = 3
            clone_succeeded = False

            for attempt in range(max_retries):
                try:
                    self._run_git_command(
                        [
                            "clone",
                            "--depth",
                            "1",  # Shallow clone - only latest commit
                            "--single-branch",
                            "--branch",
                            self._default_branch,
                            self.remote_url,
                            self.local_repo_path,
                        ],
                        cwd=os.path.dirname(self.local_repo_path),
                    )
                    logger.info(f"Repository cloned to {self.local_repo_path}")
                    clone_succeeded = True
                    break
                except subprocess.CalledProcessError as e:
                    # Check if it's an empty repository error
                    error_msg = e.stderr if e.stderr else e.stdout if e.stdout else ""
                    is_empty_repo = (
                        "does not have any commits yet" in error_msg
                        or "Could not find remote branch" in error_msg
                        or "Remote branch main not found" in error_msg
                    )

                    if is_empty_repo:
                        logger.warning(
                            f"Repository has no main branch yet. "
                            f"Initializing it locally. Error: {error_msg[:200]}"
                        )
                        # Initialize empty repo locally
                        self._initialize_empty_repo()
                        clone_succeeded = True
                        break
                    elif attempt < max_retries - 1:
                        # Repository might not be fully initialized yet
                        logger.warning(
                            f"Clone attempt {attempt + 1} failed, retrying in 5 seconds... "
                            f"Error: {error_msg[:200]}"
                        )
                        time.sleep(5)
                    else:
                        # Final attempt failed - provide detailed error
                        logger.error(
                            f"Failed to clone repository after {max_retries} attempts. "
                            f"Repository: {self.repository_url}. "
                            f"Repo existed before clone: {repo_exists}. "
                            f"Error: {error_msg}"
                        )
                        # Raise with more context
                        raise RuntimeError(
                            f"Git clone failed for existing repository {self.repository_url}. "
                            f"This likely means Git is not available or authentication failed. "
                            f"Error: {error_msg[:500]}"
                        ) from e

            if not clone_succeeded:
                raise RuntimeError(
                    f"Failed to clone or initialize repository: {self.repository_url}"
                )
        else:
            # Repository exists locally - pull latest changes
            logger.info(f"Repository already exists locally: {self.local_repo_path}")
            try:
                # Fetch latest from remote
                self._run_git_command(["fetch", "origin", self._default_branch])

                # Reset to remote main (discard any local changes)
                self._run_git_command(
                    ["reset", "--hard", f"origin/{self._default_branch}"]
                )

                logger.info("Local repository updated to latest main")
            except subprocess.CalledProcessError as e:
                # If pull fails, try to re-clone
                logger.warning(
                    f"Failed to update local repo, re-cloning: {e.stderr[:200] if e.stderr else 'unknown'}"
                )
                shutil.rmtree(self.local_repo_path)
                self._ensure_local_repo()

    def _has_uncommitted_changes(self) -> bool:
        """Check if there are uncommitted changes in the working directory."""
        result = self._run_git_command(
            ["diff", "--cached", "--quiet"], check=False, capture_output=True
        )
        return result.returncode != 0

    def _get_file_count_stats(self) -> dict[str, int]:
        """Get statistics about file changes."""
        # Get diff stats
        result = self._run_git_command(
            ["diff", "--cached", "--numstat"], capture_output=True
        )

        lines = result.stdout.strip().split("\n")
        if not lines or not lines[0]:
            return {"files_added": 0, "files_modified": 0, "files_deleted": 0}

        files_added = 0
        files_modified = 0
        files_deleted = 0

        for line in lines:
            parts = line.split("\t")
            if len(parts) >= 3:
                added, deleted = parts[0], parts[1]
                if added == "-" and deleted == "-":
                    # Binary file
                    continue
                if added != "0" and deleted == "0":
                    files_added += 1
                elif added == "0" and deleted != "0":
                    files_deleted += 1
                else:
                    files_modified += 1

        return {
            "files_added": files_added,
            "files_modified": files_modified,
            "files_deleted": files_deleted,
        }

    @override
    def write(self, filename: Path, content: str) -> None:
        """Stage a UTF-8 text file for writing in this session."""
        self._assert_open()
        if filename.is_absolute():
            raise ValueError("filename must be relative")

        key = str(filename).replace("\\", "/")
        self._staged[key] = content.encode("utf-8")

    @override
    def write_bytes(self, filename: Path, content: bytes) -> None:
        """Stage a binary file for writing in this session."""
        self._assert_open()
        if filename.is_absolute():
            raise ValueError("filename must be relative")

        key = str(filename).replace("\\", "/")
        self._staged[key] = content

    @override
    def touch(self, filename: Path) -> None:
        """Stage an empty file for creation in this session."""
        self.write_bytes(filename, b"")

    @override
    def open(self, filename: Path, mode: str = "r") -> str | bytes:
        """Read a file within this session context."""
        self._assert_open()
        if filename.is_absolute():
            raise ValueError("filename must be relative")

        key = str(filename).replace("\\", "/")

        # Check staged files first
        if key in self._staged:
            content = self._staged[key]
            return content.decode("utf-8") if mode == "r" else content

        # Try to read from the local repository, under this type's directory
        file_path = os.path.join(self._type_dir, key)
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                content = f.read()
            return content.decode("utf-8") if mode == "r" else content

        raise FileNotFoundError(f"File not found: {filename}")

    def _assert_open(self) -> None:
        """Assert that the session is still open."""
        if self._closed:
            raise RuntimeError("Storage session is closed")

    def _commit_files(self, commit_message: str) -> dict[str, Any]:
        """
        Commit the staged files on main and push.

        All of it is local git apart from the push:
        1. Clone (or update) main
        2. In REPLACE mode, delete <Type>/ in the clone
        3. Write the staged files under <Type>/
        4. Stage and commit; nothing to commit means no push
        5. Push main. A plain push is fast-forward only, so if another run
           has moved main since the clone the push is rejected and this
           run fails without touching the repository.

        There is no branch and no pull request: the commit this makes is
        the commit readers see on main, trailers and all.
        """
        if not self._staged:
            return {"skipped": True, "reason": "no files to commit"}

        self._ensure_local_repo()
        self._run_git_command(["checkout", "--quiet", self._default_branch])

        # Everything this session stages lives under <Type>/ in the
        # repository, the same layout as the local backend's
        # data/<COUNCIL>/<Type>/, so each data type has its own
        # directory and one type's run can't touch another's files.
        #
        # In REPLACE mode the latest scrape is the whole truth, so clear
        # that directory first and let the commit record the deletions.
        # In ACCUMULATE mode previous runs are history we're adding to, so
        # keep them and let git diff pick up only what actually changed.
        scraper_dir = self._type_dir
        if self.storage_mode == StorageMode.REPLACE and os.path.exists(scraper_dir):
            logger.info(f"Deleting existing files in {self.scraper_object_type}/")
            shutil.rmtree(scraper_dir)

        os.makedirs(scraper_dir, exist_ok=True)

        logger.info(f"Writing {len(self._staged)} files to local repository")
        for file_path, content in self._staged.items():
            full_path = os.path.join(scraper_dir, file_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "wb") as f:
                f.write(content)

        # Stage all changes (including deletions)
        self._run_git_command(["add", "-A", "."])

        if not self._has_uncommitted_changes():
            logger.info("No changes detected (git diff is empty)")
            return {"skipped": True, "reason": "no changes detected"}

        stats = self._get_file_count_stats()

        logger.info(f"Committing: {commit_message.splitlines()[0]}")
        self._run_git_command(["commit", "--quiet", "-m", commit_message])
        commit = self._run_git_command(["rev-parse", "HEAD"]).stdout.strip()

        logger.info(f"Pushing {self._default_branch} to remote")
        pushed = self._run_git_command(
            ["push", "--quiet", "origin", self._default_branch], check=False
        )
        if pushed.returncode != 0:
            stderr = pushed.stderr or ""
            if (
                "rejected" in stderr
                or "fetch first" in stderr
                or "non-fast-forward" in stderr
            ):
                raise RuntimeError(
                    f"{self._default_branch} of {self.repository_url} moved while "
                    "this run was scraping, so its commit was not pushed. Another "
                    "run of the same council probably overlapped this one."
                )
            raise RuntimeError(f"git push failed: {stderr[:500]}")

        logger.info(f"Pushed {commit[:10]} to {self._default_branch}")
        return {
            "success": True,
            "commit": commit,
            "files_committed": len(self._staged),
            "files_added": stats["files_added"],
            "files_modified": stats["files_modified"],
            "files_deleted": stats["files_deleted"],
        }

    def _cleanup_local_repo(self) -> None:
        """Clean up local repository directory."""
        try:
            if os.path.exists(self.local_repo_path):
                logger.info(f"Cleaning up local repository: {self.local_repo_path}")
                shutil.rmtree(self.local_repo_path)
        except Exception as e:
            logger.warning(f"Failed to clean up local repository: {e}")


class GitHubStorage(BaseStorage):
    """
    GitHub storage backend using Git CLI for operations.

    One repository per council. Each run clones main, writes this type's
    files under <Type>/, commits once with trailers naming the run, and
    pushes main fast-forward. The GitHub API is used only to create a
    repository that doesn't exist yet.
    """

    def __init__(
        self,
        council_code: str,
        scraper_object_type: str = "Data",
        organization: str | None = None,
        github_token: str | None = None,
        storage_mode: StorageMode = StorageMode.REPLACE,
    ):
        """
        Initialize GitHub storage backend.

        Args:
            council_code: Unique identifier for the council
            scraper_object_type: Type of objects being scraped
            organization: GitHub organization name
            github_token: GitHub authentication token (deprecated, ignored - GitHub App required)
        """
        super().__init__(council_code, storage_mode=storage_mode)
        self.scraper_object_type: str = scraper_object_type

        self.organization: str = organization or os.environ.get(
            "GITHUB_ORGANIZATION", ""
        )
        if not self.organization:
            raise ValueError(
                "GitHub organization not provided. Set GITHUB_ORGANIZATION environment "
                "variable or pass organization parameter."
            )

        # GitHub App authentication is required
        self.github_app = GitHubAppAuthenticator()

        if not self.github_app.is_configured():
            raise ValueError(
                "GitHub App authentication is required. Set GITHUB_APP_ID, "
                "GITHUB_APP_INSTALLATION_ID, and GITHUB_APP_PRIVATE_KEY environment variables."
            )

        logger.info(f"Using GitHub App authentication for {council_code}")
        self.github_token = self.github_app.get_token()

        self._active: _GitHubSession | None = None

    @override
    def _start_session(self, **kwargs: Any) -> StorageSession:
        """Create a new GitHub session for this council."""
        if self._active is not None:
            raise RuntimeError(
                "A session is already active on this GitHubStorage instance."
            )

        scraper_type = kwargs.get("scraper_object_type", self.scraper_object_type)
        run_id = kwargs.get("run_id")

        if not self.organization or not self.github_token:
            raise ValueError("Organization and GitHub token are required")

        # Refresh GitHub App token if needed
        if self.github_app.is_configured():
            self.github_token = self.github_app.get_token()

        session = _GitHubSession(
            organization=self.organization,
            github_token=self.github_token,
            council_code=self.council_code,
            scraper_object_type=scraper_type,
            run_id=run_id,
            storage_mode=self.storage_mode,
        )

        self._active = session
        return session

    @override
    def _end_session(self, session: StorageSession, commit_message: str, **kwargs: Any):  # type: ignore[override]
        """Commit the staged files on main and push them."""
        if not isinstance(session, _GitHubSession):
            raise RuntimeError("Unknown session type for this GitHubStorage.")

        if session is not self._active:
            if getattr(session, "_closed", False):
                return {"skipped": True, "reason": "session already finalized"}
            else:
                raise RuntimeError(
                    "Unknown or inactive session for this GitHubStorage."
                )

        if not commit_message or not commit_message.strip():
            raise ValueError("commit_message cannot be empty")

        commit_result = session._commit_files(commit_message.strip())
        if commit_result.get("skipped"):
            logger.info(
                f"Nothing to push for {session.council_code}/"
                f"{session.scraper_object_type}: {commit_result.get('reason')}"
            )
        return commit_result

    @override
    def _close_session(self, session: StorageSession | None) -> None:
        """Mark the session closed and remove the clone it made. Commits nothing."""
        if isinstance(session, _GitHubSession):
            session._closed = True
            session._cleanup_local_repo()
        self._active = None
