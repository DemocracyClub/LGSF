from __future__ import annotations

import datetime
import logging
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, override

import jwt
import requests

from lgsf.storage.backends.base import BaseStorage, StorageSession

logger = logging.getLogger(__name__)


class GitHubRateLimitError(Exception):
    """Raised when GitHub rate limit is exhausted and we should fail fast."""

    def __init__(self, message: str, reset_time: int | None = None):
        super().__init__(message)
        self.reset_time = reset_time


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

        response = requests.post(
            f"https://api.github.com/app/installations/{self.installation_id}/access_tokens",
            headers=headers,
            timeout=30,
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
    ):
        self.organization: str = organization
        self.github_token: str = github_token
        self.council_code: str = council_code
        self.scraper_object_type: str = scraper_object_type
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

        # Branch management
        self.today: str = datetime.datetime.now().strftime("%Y-%m-%d")
        self._branch_name: str | None = None
        self._default_branch: str = "main"

        # GitHub API session (only for repo creation and PR/merge)
        self.session: requests.Session = self._create_api_session()

        # CRITICAL: Check that Git is available immediately
        # This will fail the Lambda early if Git layer is missing
        self._check_git_available()

    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        return f"{str(uuid.uuid4())[:8]}-{int(time.time() * 1000) % 10000:04d}"

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
    def branch_name(self) -> str:
        """Returns unique branch name in format: {council}-{date}-{run_id}"""
        if not self._branch_name:
            self._branch_name = f"{self.council_code}-{self.today}-{self.run_id}"
        return self._branch_name

    @property
    def api_base_url(self) -> str:
        """Returns GitHub API base URL for this repository"""
        return f"https://api.github.com/repos/{self.owner}/{self.repo}"

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

        try:
            result = subprocess.run(
                cmd,
                cwd=working_dir,
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
        """Check if repository exists on GitHub using API."""
        try:
            response = self.session.get(self.api_base_url, timeout=30)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Error checking if repo exists: {e}")
            return False

    def _create_repository(self) -> None:
        """Create a new repository on GitHub using API."""
        logger.info(f"Creating new repository: {self.owner}/{self.repo}")

        create_data = {
            "name": self.repo,
            "description": f"Data storage for council {self.council_code}",
            "private": True,
            "has_issues": False,
            "has_projects": False,
            "has_wiki": False,
            "auto_init": True,  # Initialize with README to create main branch
        }

        response = self.session.post(
            f"https://api.github.com/orgs/{self.owner}/repos",
            json=create_data,
            timeout=30,
        )

        if response.status_code == 201:
            logger.info(f"Repository created successfully: {self.repository_url}")
            # Wait for GitHub to fully initialize the repository
            # Poll until we can see the main branch
            max_wait = 30  # seconds
            wait_interval = 2  # seconds
            elapsed = 0

            while elapsed < max_wait:
                time.sleep(wait_interval)
                elapsed += wait_interval

                # Check if main branch exists
                try:
                    ref_response = self.session.get(
                        f"{self.api_base_url}/git/refs/heads/{self._default_branch}",
                        timeout=10,
                    )
                    if ref_response.status_code == 200:
                        logger.info(
                            f"Repository fully initialized after {elapsed} seconds"
                        )
                        return
                except Exception:
                    pass

                logger.debug(
                    f"Waiting for repository initialization... ({elapsed}s elapsed)"
                )

            logger.warning(
                f"Repository initialization taking longer than expected ({max_wait}s), "
                "proceeding anyway..."
            )
        elif response.status_code == 422:
            # Repository might already exist
            logger.info(f"Repository might already exist: {self.owner}/{self.repo}")
        else:
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

        # Create initial README
        readme_path = os.path.join(self.local_repo_path, "README.md")
        with open(readme_path, "w") as f:
            f.write(
                f"# {self.repo}\n\n"
                f"Data repository for {self.council_code}\n\n"
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
        """
        if not os.path.exists(self.local_repo_path):
            # Need to clone - first ensure repo exists on GitHub
            repo_exists = self._repo_exists_on_github()
            if not repo_exists:
                self._create_repository()
                repo_exists = True

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

                    # Only initialize empty repos if we JUST created them (repo_exists was False)
                    if is_empty_repo and not repo_exists:
                        logger.warning(
                            f"Newly created repository has no main branch yet. "
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

    def _create_and_checkout_branch(self) -> None:
        """Create a new branch from main and check it out."""
        logger.info(f"Creating and checking out branch: {self.branch_name}")

        # Ensure we're on main first
        self._run_git_command(["checkout", self._default_branch])

        # Create and checkout new branch
        self._run_git_command(["checkout", "-b", self.branch_name])

        logger.info(f"Now on branch: {self.branch_name}")

    def _has_uncommitted_changes(self) -> bool:
        """Check if there are uncommitted changes in the working directory."""
        result = self._run_git_command(
            ["diff", "--cached", "--quiet"], check=False, capture_output=True
        )
        return result.returncode != 0

    def _has_changes_from_main(self) -> bool:
        """Check if current branch has changes compared to main."""
        result = self._run_git_command(
            ["diff", "--quiet", f"origin/{self._default_branch}"],
            check=False,
            capture_output=True,
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

        # Try to read from local repository
        file_path = os.path.join(self.local_repo_path, key)
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
        Commit all staged files to the local Git repository.

        This performs all operations locally using Git CLI:
        1. Ensure local repo is ready
        2. Create new branch
        3. Delete all files in scraper folder
        4. Write new files
        5. Stage and commit
        6. Check for changes
        7. Push to remote
        8. Create PR and merge
        """
        if not self._staged:
            return {"skipped": True, "reason": "no files to commit"}

        try:
            # Ensure local repository exists and is up to date
            self._ensure_local_repo()

            # Create and checkout new branch
            self._create_and_checkout_branch()

            # Delete all existing files in the scraper object type folder
            scraper_dir = os.path.join(self.local_repo_path, self.scraper_object_type)
            if os.path.exists(scraper_dir):
                logger.info(f"Deleting existing files in {self.scraper_object_type}/")
                shutil.rmtree(scraper_dir)

            # Create directory
            os.makedirs(scraper_dir, exist_ok=True)

            # Write all staged files to disk
            logger.info(f"Writing {len(self._staged)} files to local repository")
            for file_path, content in self._staged.items():
                full_path = os.path.join(self.local_repo_path, file_path)
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                with open(full_path, "wb") as f:
                    f.write(content)

            # Stage all changes (including deletions)
            self._run_git_command(["add", "-A", "."])

            # Check if there are any changes
            if not self._has_uncommitted_changes():
                logger.info("No changes detected (git diff is empty)")
                return {"skipped": True, "reason": "no changes detected"}

            # Get file statistics before committing
            stats = self._get_file_count_stats()

            # Commit changes
            logger.info(f"Committing changes: {commit_message}")
            self._run_git_command(["commit", "-m", commit_message])

            # Double-check: compare with remote main
            if not self._has_changes_from_main():
                logger.info("No changes compared to remote main")
                return {"skipped": True, "reason": "no changes from main"}

            # Push branch to remote
            logger.info(f"Pushing branch {self.branch_name} to remote")
            self._run_git_command(["push", "origin", self.branch_name])

            logger.info(f"Successfully pushed branch: {self.branch_name}")

            return {
                "success": True,
                "branch": self.branch_name,
                "files_committed": len(self._staged),
                "files_added": stats["files_added"],
                "files_modified": stats["files_modified"],
                "files_deleted": stats["files_deleted"],
            }

        except subprocess.CalledProcessError as e:
            logger.error(f"Git operation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during commit: {e}")
            raise

    def _create_pull_request(self, commit_result: dict[str, Any]) -> dict[str, Any]:
        """Create a pull request and merge it using GitHub API."""
        try:
            # Create pull request
            pr_data = {
                "title": f"Update {self.council_code} data ({self.today})",
                "head": self.branch_name,
                "base": self._default_branch,
                "body": (
                    f"Automated update of {self.council_code} scraper data for {self.today}\n\n"
                    f"**Changes:**\n"
                    f"- Files committed: {commit_result.get('files_committed', 0)}\n"
                    f"- Files added: {commit_result.get('files_added', 0)}\n"
                    f"- Files modified: {commit_result.get('files_modified', 0)}\n"
                    f"- Files deleted: {commit_result.get('files_deleted', 0)}"
                ),
            }

            logger.info(
                f"Creating pull request: {self.branch_name} -> {self._default_branch}"
            )
            pr_response = self.session.post(
                f"{self.api_base_url}/pulls", json=pr_data, timeout=30
            )
            pr_response.raise_for_status()
            pr_data_result = pr_response.json()
            pr_number = pr_data_result["number"]
            pr_url = pr_data_result["html_url"]

            logger.info(f"Pull request created: {pr_url}")

            # Merge pull request
            merge_data = {
                "commit_title": f"Merge {self.council_code} data ({self.today})",
                "commit_message": f"Automated merge of {self.council_code} data",
                "merge_method": "merge",  # Use merge commit (not squash or rebase)
            }

            logger.info(f"Merging pull request #{pr_number}")
            merge_response = self.session.put(
                f"{self.api_base_url}/pulls/{pr_number}/merge",
                json=merge_data,
                timeout=30,
            )
            merge_response.raise_for_status()

            logger.info("Pull request merged successfully")

            return {
                "success": True,
                "pr_number": pr_number,
                "pr_url": pr_url,
                "merged": True,
            }

        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to create/merge PR: {e}")
            if e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return {
                "success": False,
                "error": str(e),
                "response": e.response.text if e.response else None,
            }
        except Exception as e:
            logger.error(f"Unexpected error creating/merging PR: {e}")
            return {"success": False, "error": str(e)}

    def _delete_remote_branch(self) -> dict[str, Any]:
        """Delete the branch from remote repository."""
        try:
            logger.info(f"Deleting remote branch: {self.branch_name}")
            delete_url = f"{self.api_base_url}/git/refs/heads/{self.branch_name}"
            response = self.session.delete(delete_url, timeout=30)
            response.raise_for_status()

            logger.info(f"Branch deleted successfully: {self.branch_name}")
            return {"success": True, "branch": self.branch_name}

        except requests.exceptions.HTTPError as e:
            logger.warning(f"Failed to delete branch {self.branch_name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "branch": self.branch_name,
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

    This storage backend implements the BaseStorage interface using local Git
    operations to minimize API calls and avoid rate limiting issues.
    """

    def __init__(
        self,
        council_code: str,
        scraper_object_type: str = "Data",
        organization: str | None = None,
        github_token: str | None = None,
        auto_merge: bool = True,
    ):
        """
        Initialize GitHub storage backend.

        Args:
            council_code: Unique identifier for the council
            scraper_object_type: Type of objects being scraped
            organization: GitHub organization name
            github_token: GitHub authentication token (deprecated, ignored - GitHub App required)
            auto_merge: If True, automatically merge PRs after creation (always True in this version)
        """
        super().__init__(council_code)
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
        self.auto_merge: bool = auto_merge  # Always True in this version

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
        )

        self._active = session
        return session

    @override
    def _end_session(self, session: StorageSession, commit_message: str, **kwargs: Any):  # type: ignore[override]
        """Commit all staged changes and perform GitHub finalization with PR workflow."""
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

        try:
            # Commit and push the staged files
            commit_result = session._commit_files(commit_message.strip())

            if commit_result.get("skipped"):
                skip_reason = commit_result.get("reason", "unknown")
                logger.info(
                    f"Skipping GitHub operations for {session.council_code}/"
                    f"{session.scraper_object_type}: {skip_reason}"
                )
                return commit_result

            finalization_result = commit_result.copy()

            # Create PR and merge (always done, unless skip_merge is set)
            if not kwargs.get("skip_merge", False):
                pr_result = session._create_pull_request(commit_result)
                finalization_result["pull_request"] = pr_result

                # Clean up remote branch if PR was merged
                if pr_result.get("success") and pr_result.get("merged", False):
                    branch_cleanup = session._delete_remote_branch()
                    finalization_result["branch_cleanup"] = branch_cleanup
                else:
                    finalization_result["branch_cleanup"] = {"skipped": "pr_not_merged"}

            return finalization_result

        finally:
            # Always clean up local repository and reset session
            session._cleanup_local_repo()
            self._reset_session_state(session)

    @override
    def _reset_session_state(self, session: StorageSession | None) -> None:
        """Reset session state."""
        if isinstance(session, _GitHubSession):
            session._closed = True
        self._active = None
