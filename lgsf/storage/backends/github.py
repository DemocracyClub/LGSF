from __future__ import annotations

import base64
import datetime
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable, override

import requests

from lgsf.storage.backends.base import BaseStorage, StorageSession

logger = logging.getLogger(__name__)


class _GitHubSession(StorageSession):
    """
    GitHub session implementation for GitHub repositories.

    This session implementation stages all file operations in memory and creates
    a single commit with all changes when the session is committed.
    """

    def __init__(
        self,
        organization: str,
        github_token: str,
        council_code: str,
        scraper_object_type: str = "Data",
        run_id: str | None = None,
        disable_change_detection: bool = False,
    ):
        self.organization: str = organization
        self.github_token: str = github_token
        self.council_code: str = council_code
        self.scraper_object_type: str = scraper_object_type
        self.run_id: str = run_id or self._generate_run_id()
        self.disable_change_detection: bool = disable_change_detection

        # Repository configuration
        self.owner: str = organization
        self.repo: str = council_code.upper()
        self.repository_url: str = f"https://github.com/{self.owner}/{self.repo}"

        # Session state
        self._staged: dict[str, bytes] = {}
        self._closed: bool = False
        self._existing_files: list[str] = []
        self._preparation_result: dict[str, Any] | None = None

        # Branch management
        self.today: str = datetime.datetime.now().strftime("%Y-%m-%d")
        self._branch_name: str | None = None
        self._base_sha: str | None = None

        # GitHub API session
        self.session: requests.Session = self._create_api_session()
        self._ensure_repository_exists()

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
                "User-Agent": "LGSF-GitHub-Storage/1.0",
            }
        )
        return session

    def _normalize_path(self, filename: Path) -> str:
        """Normalize file path for cross-platform compatibility."""
        return str(filename).replace("\\", "/")

    def _retry_request(
        self,
        func: Callable[..., requests.Response],
        max_retries: int = 3,
        *args: Any,
        **kwargs: Any,
    ) -> requests.Response:
        """Retry a request function with exponential backoff for network issues."""
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ):
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                    continue
                raise
        raise RuntimeError("Should not reach here")

    def _handle_api_error(self, response: requests.Response, context: str) -> bool:
        """
        Handle common GitHub API errors with helpful messages.

        Returns:
            bool: True if error was handled and caller should continue, False if should raise
        """
        if response.status_code == 403:
            logger.warning(
                f"GitHub API access forbidden during {context} for {self.council_code}. "
                "Check token permissions (needs 'repo' scope)"
            )
            return True
        elif response.status_code == 404:
            logger.warning(
                f"Resource not found during {context} for {self.council_code}"
            )
            return True

        return False

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

    def _ensure_repository_exists(self) -> None:
        """Ensure the council's repository exists, create it if it doesn't."""
        response = self.session.get(self.api_base_url)

        if response.status_code == 200:
            return
        elif response.status_code == 404:
            self._create_repository()
        else:
            response.raise_for_status()

    def _create_repository(self) -> None:
        """Create a new repository for this council."""
        create_data = {
            "name": self.repo,
            "description": f"Data storage for council {self.council_code}",
            "private": True,
            "has_issues": False,
            "has_projects": False,
            "has_wiki": False,
            "auto_init": True,
        }

        response = self.session.post(
            f"https://api.github.com/orgs/{self.owner}/repos", json=create_data
        )

        if response.status_code == 201:
            time.sleep(5)  # Wait for repository initialization
        else:
            response.raise_for_status()

    def _get_default_branch_name(self) -> str:
        """Get the name of the default branch."""
        response = self.session.get(self.api_base_url)
        response.raise_for_status()
        return response.json()["default_branch"]

    def _is_repository_empty(self) -> bool:
        """Check if the repository is empty (no commits)"""
        try:
            response = self.session.get(self.api_base_url)
            response.raise_for_status()
            return response.json().get("size", 0) == 0
        except Exception:
            return True

    def _create_initial_commit(self) -> str:
        """Create initial commit with README.md."""
        readme_content = (
            "# LGSF Data Repository\n\n"
            "This repository contains scraped data from the "
            "Local Government Scraper Framework (LGSF)."
        )

        # Create blob
        blob_data = {"content": readme_content, "encoding": "utf-8"}
        blob_response = self._retry_request(
            self.session.post, 3, f"{self.api_base_url}/git/blobs", json=blob_data
        )

        if blob_response.status_code != 201:
            if self._handle_api_error(blob_response, "blob creation"):
                logger.warning("Failed to create initial commit blob, but continuing")
            else:
                blob_response.raise_for_status()

        blob_sha = blob_response.json()["sha"]

        # Create tree
        tree_data = {
            "tree": [
                {"path": "README.md", "mode": "100644", "type": "blob", "sha": blob_sha}
            ]
        }
        tree_response = self._retry_request(
            self.session.post, 3, f"{self.api_base_url}/git/trees", json=tree_data
        )
        tree_response.raise_for_status()
        tree_sha = tree_response.json()["sha"]

        # Create commit
        commit_data = {
            "message": "Initial commit - LGSF data repository",
            "tree": tree_sha,
            "parents": [],
        }
        commit_response = self._retry_request(
            self.session.post, 3, f"{self.api_base_url}/git/commits", json=commit_data
        )
        commit_response.raise_for_status()
        commit_sha = commit_response.json()["sha"]

        # Create main branch reference
        ref_data = {"ref": "refs/heads/main", "sha": commit_sha}
        ref_response = self._retry_request(
            self.session.post, 3, f"{self.api_base_url}/git/refs", json=ref_data
        )
        ref_response.raise_for_status()

        return commit_sha

    def _get_branch_sha(self, branch_name: str) -> str:
        """Get the SHA of a specific branch."""
        url = f"{self.api_base_url}/git/refs/heads/{branch_name}"
        response = self.session.get(url)

        if response.status_code == 409 and self._is_repository_empty():
            self._create_initial_commit()
            response = self.session.get(url)

        response.raise_for_status()
        return response.json()["object"]["sha"]

    def _create_branch(self) -> str:
        """Create a new branch from the default branch and return its SHA."""
        default_branch = self._get_default_branch_name()
        default_branch_sha = self._get_branch_sha(default_branch)

        create_ref_data = {
            "ref": f"refs/heads/{self.branch_name}",
            "sha": default_branch_sha,
        }

        try:
            response = self.session.post(
                f"{self.api_base_url}/git/refs", json=create_ref_data
            )
            response.raise_for_status()
            return default_branch_sha
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logger.warning(
                    "Insufficient permissions to create branch, using default branch"
                )
                self._branch_name = default_branch
                return default_branch_sha
            elif e.response.status_code == 422:
                # Branch already exists or conflict - generate new ID and retry
                try:
                    return self._get_branch_sha(self.branch_name)
                except:
                    self.run_id = self._generate_run_id()
                    self._branch_name = None
                    return self._create_branch()
            raise

    def _ensure_branch_exists(self) -> str:
        """Ensure the branch exists and return its base SHA."""
        if self._base_sha:
            return self._base_sha

        try:
            self._base_sha = self._get_branch_sha(self.branch_name)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self._base_sha = self._create_branch()
            elif e.response.status_code == 409:
                self.run_id = self._generate_run_id()
                self._branch_name = None
                self._base_sha = self._create_branch()
            else:
                raise

        return self._base_sha

    def _cleanup_old_branches(self) -> dict[str, Any]:
        """Clean up old branches created by this council's scrapers."""
        try:
            response = self.session.get(f"{self.api_base_url}/git/refs/heads")
            response.raise_for_status()

            branches = response.json()
            if not isinstance(branches, list):
                return {"cleaned_up": 0}

            council_prefix = f"{self.council_code}-"
            current_branch = self.branch_name
            cleanup_count = 0

            for branch in branches:
                branch_name = branch["ref"].replace("refs/heads/", "")

                if (
                    not branch_name.startswith(council_prefix)
                    or branch_name == current_branch
                ):
                    continue

                try:
                    delete_response = self.session.delete(
                        f"{self.api_base_url}/git/refs/heads/{branch_name}"
                    )
                    if delete_response.status_code in [200, 204]:
                        cleanup_count += 1
                except Exception:
                    continue

            return {"cleaned_up": cleanup_count}

        except Exception as e:
            return {"error": str(e), "cleaned_up": 0}

    def _collect_existing_files(self, path: str, default_branch: str) -> None:
        """Recursively collect all files in the scraper type folder."""
        try:
            response = self.session.get(
                f"{self.api_base_url}/contents/{path}", params={"ref": default_branch}
            )
            if response.status_code != 200:
                return

            items = response.json()
            if not isinstance(items, list):
                items = [items]

            for item in items:
                if item["type"] == "file":
                    self._existing_files.append(item["path"])
                elif item["type"] == "dir":
                    self._collect_existing_files(item["path"], default_branch)
        except requests.exceptions.HTTPError:
            pass

    def _delete_existing_data_if_needed(self) -> dict[str, Any]:
        """Get information about existing data that will be replaced."""
        try:
            default_branch = self._get_default_branch_name()
            folder_path = self.scraper_object_type
            self._existing_files = []

            self._collect_existing_files(folder_path, default_branch)

            return {
                "existing_files_found": len(self._existing_files),
                "files_to_replace": self._existing_files,
            }

        except Exception as e:
            self._existing_files = []
            return {"error": str(e), "will_proceed": True}

    @override
    def write(self, filename: Path, content: str) -> None:
        """Stage a UTF-8 text file for writing in this session."""
        self._assert_open()
        if filename.is_absolute():
            raise ValueError("filename must be relative")

        key = self._normalize_path(filename)
        self._staged[key] = content.encode("utf-8")

    @override
    def write_bytes(self, filename: Path, content: bytes) -> None:
        """Stage a binary file for writing in this session."""
        self._assert_open()
        if filename.is_absolute():
            raise ValueError("filename must be relative")

        key = self._normalize_path(filename)
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

        key = self._normalize_path(filename)

        # Check staged files first
        if key in self._staged:
            content = self._staged[key]
            return content.decode("utf-8") if mode == "r" else content

        # Try to read from repository
        try:
            response = self.session.get(f"{self.api_base_url}/contents/{key}")
            if response.status_code == 200:
                file_data = response.json()
                content = base64.b64decode(file_data["content"])
                return content.decode("utf-8") if mode == "r" else content
            raise FileNotFoundError(f"File not found: {filename}")
        except requests.exceptions.HTTPError:
            raise FileNotFoundError(f"File not found: {filename}")

    def _assert_open(self) -> None:
        """Assert that the session is still open."""
        if self._closed:
            raise RuntimeError("Storage session is closed")

    def _commit_files(self, commit_message: str) -> dict[str, Any]:
        """Commit all staged files to the GitHub repository."""
        if not self._staged:
            return {"skipped": True, "reason": "no files to commit"}

        if not self.disable_change_detection and not self._detect_changes():
            return {"skipped": True, "reason": "no changes detected"}

        if self.disable_change_detection:
            logger.info(
                f"Change detection disabled - proceeding with commit for "
                f"{self.council_code}/{self.scraper_object_type}"
            )

        self._ensure_branch_exists()

        # Get current tree SHA
        current_commit_response = self.session.get(
            f"{self.api_base_url}/git/commits/{self._base_sha}"
        )
        current_commit_response.raise_for_status()
        current_tree_sha = current_commit_response.json()["tree"]["sha"]

        # Create blobs for all staged files
        blobs = {}
        for file_path, content in self._staged.items():
            blob_data = {
                "content": base64.b64encode(content).decode("ascii"),
                "encoding": "base64",
            }
            blob_response = self._retry_request(
                self.session.post, 3, f"{self.api_base_url}/git/blobs", json=blob_data
            )
            blob_response.raise_for_status()
            blobs[file_path] = blob_response.json()["sha"]

        # Create new tree with staged files and deletions
        tree_items = []

        # Add all staged files
        for file_path, blob_sha in blobs.items():
            tree_items.append(
                {"path": file_path, "mode": "100644", "type": "blob", "sha": blob_sha}
            )

        # Delete existing files that aren't being updated
        for existing_file_path in self._existing_files:
            if existing_file_path not in self._staged:
                tree_items.append(
                    {
                        "path": existing_file_path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": None,
                    }
                )

        tree_data = {"base_tree": current_tree_sha, "tree": tree_items}
        tree_response = self._retry_request(
            self.session.post, 3, f"{self.api_base_url}/git/trees", json=tree_data
        )
        tree_response.raise_for_status()
        new_tree_sha = tree_response.json()["sha"]

        # Create commit
        commit_data = {
            "message": commit_message,
            "tree": new_tree_sha,
            "parents": [self._base_sha],
        }
        commit_response = self._retry_request(
            self.session.post, 3, f"{self.api_base_url}/git/commits", json=commit_data
        )
        commit_response.raise_for_status()
        new_commit_sha = commit_response.json()["sha"]

        # Update branch reference
        update_ref_data = {"sha": new_commit_sha, "force": False}
        update_response = self._retry_request(
            self.session.patch,
            3,
            f"{self.api_base_url}/git/refs/heads/{self.branch_name}",
            json=update_ref_data,
        )
        update_response.raise_for_status()

        # Calculate deletion count
        deleted_count = sum(
            1 for path in self._existing_files if path not in self._staged
        )

        return {
            "files_committed": len(self._staged),
            "files_deleted": deleted_count,
            "commit_sha": new_commit_sha,
            "branch": self.branch_name,
        }

    def _detect_changes(self) -> bool:
        """
        Detect if there are any actual changes between staged files and existing files.

        Returns True if changes detected, False if no changes.
        If we can't access the repository, we assume changes exist.
        """
        try:
            repo_response = self.session.get(self.api_base_url)

            if self._handle_api_error(repo_response, "change detection"):
                return True

            repo_response.raise_for_status()
            default_branch = repo_response.json()["default_branch"]

            logger.info(
                f"Change detection for {self.council_code}/{self.scraper_object_type}: "
                f"{len(self._staged)} staged files, {len(self._existing_files)} existing files"
            )

            # Check if any files will be deleted
            files_to_delete = [
                path for path in self._existing_files if path not in self._staged
            ]

            if files_to_delete:
                logger.info(f"Changes detected: {len(files_to_delete)} files to delete")
                return True

            # Check if any staged files are new or have different content
            files_checked = 0

            for file_path, staged_content in self._staged.items():
                try:
                    response = self.session.get(
                        f"{self.api_base_url}/contents/{file_path}",
                        params={"ref": default_branch},
                    )

                    if self._handle_api_error(response, f"file check for {file_path}"):
                        return True

                    if response.status_code == 404:
                        logger.info(f"Changes detected: new file {file_path}")
                        return True
                    elif response.status_code == 200:
                        existing_file_data = response.json()
                        existing_content = base64.b64decode(
                            existing_file_data["content"]
                        )

                        if existing_content != staged_content:
                            logger.info(f"Changes detected: modified file {file_path}")
                            return True

                        files_checked += 1
                    else:
                        logger.warning(
                            f"Unable to check file {file_path} "
                            f"(HTTP {response.status_code}) - assuming changes exist"
                        )
                        return True

                except requests.exceptions.RequestException as e:
                    logger.warning(
                        f"Network error checking file {file_path}: {e} - assuming changes exist"
                    )
                    return True
                except Exception as e:
                    logger.warning(
                        f"Error checking file {file_path}: {e} - assuming changes exist"
                    )
                    return True

            logger.info(
                f"No changes detected: all {files_checked}/{len(self._staged)} "
                "staged files match existing content"
            )
            return False

        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Network error during change detection: {e} - assuming changes exist"
            )
            return True
        except Exception as e:
            logger.warning(
                f"Error during change detection: {e} - assuming changes exist"
            )
            return True


class GitHubStorage(BaseStorage):
    """
    GitHub storage backend with git-based operations and per-run branching.

    This storage backend implements the BaseStorage interface using GitHub
    repositories for council data storage.
    """

    def __init__(
        self,
        council_code: str,
        scraper_object_type: str = "Data",
        organization: str | None = None,
        github_token: str | None = None,
        auto_merge: bool = True,
        disable_change_detection: bool = False,
    ):
        """
        Initialize GitHub storage backend.

        Args:
            council_code: Unique identifier for the council
            scraper_object_type: Type of objects being scraped
            organization: GitHub organization name
            github_token: GitHub authentication token
            auto_merge: If True, automatically merge PRs after creation
            disable_change_detection: If True, skip change detection and always commit
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

        self.github_token: str = github_token or os.environ.get("GITHUB_TOKEN", "")
        if not self.github_token:
            raise ValueError(
                "GitHub token not provided. Set GITHUB_TOKEN environment variable "
                "or pass github_token parameter."
            )

        self._active: _GitHubSession | None = None
        self.auto_merge: bool = auto_merge

        # Check environment variable for disabling change detection
        env_disable_change_detection = os.environ.get(
            "LGSF_DISABLE_CHANGE_DETECTION", ""
        ).lower() in ("true", "1", "yes")
        self.disable_change_detection: bool = (
            disable_change_detection or env_disable_change_detection
        )

        if self.disable_change_detection:
            logger.info(
                "Change detection disabled via configuration or "
                "LGSF_DISABLE_CHANGE_DETECTION environment variable"
            )

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

        session = _GitHubSession(
            organization=self.organization,
            github_token=self.github_token,
            council_code=self.council_code,
            scraper_object_type=scraper_type,
            run_id=run_id,
            disable_change_detection=self.disable_change_detection,
        )

        # Check for existing data
        delete_result = session._delete_existing_data_if_needed()
        if delete_result.get("existing_files_found", 0) > 0:
            session._preparation_result = delete_result

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
            # Commit the staged files
            commit_result = session._commit_files(commit_message.strip())

            if commit_result.get("skipped"):
                skip_reason = commit_result.get("reason", "unknown")
                logger.info(
                    f"Skipping GitHub operations for {session.council_code}/"
                    f"{session.scraper_object_type}: {skip_reason}"
                )
                return commit_result

            finalization_result = commit_result.copy()

            if kwargs.get("skip_merge", False):
                return finalization_result

            # Create PR and optionally merge
            pr_result = self._create_pull_request(
                session, max_retries=kwargs.get("max_merge_retries", 3)
            )
            finalization_result["pull_request"] = pr_result

            # Clean up branch if PR was merged
            if pr_result.get("success") and pr_result.get("merged", False):
                cleanup_result = self._delete_branch(session)
                finalization_result["branch_cleanup"] = cleanup_result

                old_branches_cleanup = session._cleanup_old_branches()
                finalization_result["old_branches_cleanup"] = old_branches_cleanup
            elif pr_result.get("success") and not pr_result.get("merged", True):
                finalization_result["branch_cleanup"] = {"skipped": "pr_not_merged"}
                finalization_result["old_branches_cleanup"] = {
                    "skipped": "pr_not_merged"
                }
            else:
                finalization_result["branch_cleanup"] = {
                    "skipped": "pr_creation_failed"
                }
                finalization_result["old_branches_cleanup"] = {
                    "skipped": "pr_creation_failed"
                }

            return finalization_result

        finally:
            self._reset_session_state(session)

    @override
    def _reset_session_state(self, session: StorageSession | None) -> None:
        """Reset session state."""
        if isinstance(session, _GitHubSession):
            session._closed = True
        self._active = None

    def _create_pull_request(
        self, session: _GitHubSession, max_retries: int = 3
    ) -> dict[str, Any]:
        """Create a pull request and optionally merge it to the main branch."""
        for attempt in range(max_retries + 1):
            try:
                default_branch = session._get_default_branch_name()

                # Create pull request
                pr_data = {
                    "title": f"Merge {session.council_code} data ({session.today})",
                    "head": session.branch_name,
                    "base": default_branch,
                    "body": f"Automated merge of {session.council_code} scraper data for {session.today}",
                }

                pr_response = session.session.post(
                    f"{session.api_base_url}/pulls", json=pr_data
                )
                pr_response.raise_for_status()
                pr_data_result = pr_response.json()
                pr_number = pr_data_result["number"]
                pr_url = pr_data_result["html_url"]

                # Only merge if auto_merge is enabled
                if self.auto_merge:
                    merge_data = {
                        "commit_title": f"Merge {session.council_code} data ({session.today})",
                        "merge_method": "merge",
                    }

                    merge_response = session.session.put(
                        f"{session.api_base_url}/pulls/{pr_number}/merge",
                        json=merge_data,
                    )
                    merge_response.raise_for_status()

                    return {
                        "success": True,
                        "pr_number": pr_number,
                        "pr_url": pr_url,
                        "merged": True,
                        "attempt": attempt + 1,
                    }
                else:
                    return {
                        "success": True,
                        "pr_number": pr_number,
                        "pr_url": pr_url,
                        "merged": False,
                        "attempt": attempt + 1,
                    }

            except requests.exceptions.HTTPError as e:
                if (
                    e.response
                    and e.response.status_code == 409
                    and attempt < max_retries
                    and self.auto_merge
                ):
                    time.sleep(2**attempt)  # Exponential backoff
                    continue
                else:
                    return {"success": False, "error": str(e), "attempt": attempt + 1}

        return {
            "success": False,
            "error": "Maximum merge retries exceeded",
            "attempts": max_retries + 1,
        }

    def _delete_branch(self, session: _GitHubSession) -> dict[str, Any]:
        """Delete the session's branch after successful merge."""
        try:
            delete_url = f"{session.api_base_url}/git/refs/heads/{session.branch_name}"
            response = session.session.delete(delete_url)
            response.raise_for_status()

            return {"success": True, "branch": session.branch_name}

        except requests.exceptions.HTTPError as e:
            return {"success": False, "error": str(e), "branch": session.branch_name}
