from __future__ import annotations

import datetime
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests


from lgsf.storage.backends.base import BaseStorage, StorageSession


class _GitHubSession(StorageSession):
    """
    GitHub session implementation for GitHub repositories.

    This session implementation stages all file operations in memory and creates
    a single commit with all changes when the session is committed. It provides:

    - **Git-based operations**: All files are committed to a git repository
    - **Per-run branches**: Operations happen on unique run-based branches
    - **Atomic commits**: All staged files are committed together
    - **Race condition handling**: Uses unique branch names to avoid conflicts
    - **Single repository**: All councils share one repository with separate folders

    The session works with GitHub's API to provide git-like semantics
    while maintaining the storage session interface.

    Args:
        repository_url: Full GitHub repository URL (owner/repo format)
        github_token: GitHub authentication token
        council_code: Council identifier for folder isolation
        scraper_object_type: Type of scraper (e.g., "Councillors")
        run_id: Unique identifier for this scraper run
    """

    def __init__(
        self,
        repository_url: str,
        github_token: str,
        council_code: str,
        scraper_object_type: str = "Data",
        run_id: Optional[str] = None,
        storage_backend: Optional[Any] = None,
    ):
        self.repository_url: str = repository_url.rstrip("/")
        self.github_token: str = github_token
        self.council_code: str = council_code
        self.scraper_object_type: str = scraper_object_type
        self.storage_backend: Optional[Any] = storage_backend
        self.run_id: str = (
            run_id or f"{str(uuid.uuid4())[:8]}-{int(time.time() * 1000) % 10000:04d}"
        )

        # Parse repository URL to get owner and repo name
        if self.repository_url.startswith("https://github.com/"):
            repo_path = self.repository_url.replace("https://github.com/", "")
        else:
            repo_path = self.repository_url

        parts = repo_path.split("/")
        if len(parts) != 2:
            raise ValueError(
                f"Invalid repository URL format. Expected 'owner/repo', got: {repo_path}"
            )

        self.owner: str = parts[0]
        self.repo: str = parts[1]

        self._staged: Dict[str, bytes] = {}
        self._closed: bool = False
        self._existing_files: List[str] = []  # Track existing files for deletion

        # Branch management
        self.today: str = datetime.datetime.now().strftime("%Y-%m-%d")
        self._branch_name: Optional[str] = None
        self._base_sha: Optional[str] = None

        # GitHub API session
        self.session: requests.Session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"token {self.github_token}",
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "LGSF-GitHub-Storage/1.0",
            }
        )

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

    def _get_default_branch_sha(self) -> str:
        """Get the SHA of the default branch (usually main or master)"""
        url = f"{self.api_base_url}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()["default_branch"]

    def _is_repository_empty(self) -> bool:
        """Check if the repository is empty (no commits)"""
        try:
            repo_url = f"{self.api_base_url}"
            repo_response = self.session.get(repo_url)
            repo_response.raise_for_status()
            repo_data = repo_response.json()
            return repo_data.get("size", 0) == 0
        except:
            return True

    def _create_initial_commit(self) -> str:
        """Create initial commit with README.md using Git objects API"""
        # Create README.md blob
        readme_content = "# LGSF Data Repository\n\nThis repository contains scraped data from the Local Government Scraper Framework (LGSF)."

        blob_data = {"content": readme_content, "encoding": "utf-8"}

        blob_response = self.session.post(
            f"{self.api_base_url}/git/blobs", json=blob_data
        )
        blob_response.raise_for_status()
        blob_sha = blob_response.json()["sha"]

        # Create tree with README.md
        tree_data = {
            "tree": [
                {"path": "README.md", "mode": "100644", "type": "blob", "sha": blob_sha}
            ]
        }

        tree_response = self.session.post(
            f"{self.api_base_url}/git/trees", json=tree_data
        )
        tree_response.raise_for_status()
        tree_sha = tree_response.json()["sha"]

        # Create initial commit (no parents for first commit)
        commit_data = {
            "message": "Initial commit - LGSF data repository",
            "tree": tree_sha,
            "parents": [],
        }

        commit_response = self.session.post(
            f"{self.api_base_url}/git/commits", json=commit_data
        )
        commit_response.raise_for_status()
        commit_sha = commit_response.json()["sha"]

        # Create main branch reference
        ref_data = {"ref": "refs/heads/main", "sha": commit_sha}

        ref_response = self.session.post(f"{self.api_base_url}/git/refs", json=ref_data)
        ref_response.raise_for_status()

        return commit_sha

    def _get_branch_sha(self, branch_name: str) -> str:
        """Get the SHA of a specific branch"""
        url = f"{self.api_base_url}/git/refs/heads/{branch_name}"
        response = self.session.get(url)

        if response.status_code == 409:
            # Repository might be empty
            if self._is_repository_empty():
                # Create initial commit first
                self._create_initial_commit()
                # Try again
                response = self.session.get(url)

        response.raise_for_status()
        return response.json()["object"]["sha"]

    def _create_branch(self) -> str:
        """Create a new branch from the default branch and return its SHA"""
        # Get default branch info
        repo_url = f"{self.api_base_url}"
        repo_response = self.session.get(repo_url)
        repo_response.raise_for_status()
        default_branch = repo_response.json()["default_branch"]

        # Get the SHA of the default branch
        default_branch_sha = self._get_branch_sha(default_branch)

        # Create new branch
        create_ref_url = f"{self.api_base_url}/git/refs"
        create_ref_data = {
            "ref": f"refs/heads/{self.branch_name}",
            "sha": default_branch_sha,
        }

        try:
            response = self.session.post(create_ref_url, json=create_ref_data)
            response.raise_for_status()
            return default_branch_sha
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                # Branch already exists, try to get its SHA
                try:
                    return self._get_branch_sha(self.branch_name)
                except:
                    # Generate new unique branch name and try again
                    self.run_id = (
                        f"{str(uuid.uuid4())[:8]}-{int(time.time() * 1000) % 10000:04d}"
                    )
                    self._branch_name = None  # Reset cached branch name
                    create_ref_data["ref"] = f"refs/heads/{self.branch_name}"
                    response = self.session.post(create_ref_url, json=create_ref_data)
                    response.raise_for_status()
                    return default_branch_sha
            else:
                raise

    def _ensure_branch_exists(self) -> str:
        """Ensure the branch exists and return its base SHA"""
        if self._base_sha:
            return self._base_sha

        try:
            # Try to get existing branch
            self._base_sha = self._get_branch_sha(self.branch_name)
            return self._base_sha
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Branch doesn't exist, create it
                self._base_sha = self._create_branch()
                return self._base_sha
            elif e.response.status_code == 409:
                # Some other conflict, try with new branch name
                self.run_id = (
                    f"{str(uuid.uuid4())[:8]}-{int(time.time() * 1000) % 10000:04d}"
                )
                self._branch_name = None  # Reset cached branch name
                self._base_sha = self._create_branch()
                return self._base_sha
            else:
                raise

        return self._base_sha

    def _cleanup_old_branches(self):
        """Clean up old branches created by this council's scrapers"""
        try:
            # Get all branches
            branches_url = f"{self.api_base_url}/git/refs/heads"
            response = self.session.get(branches_url)
            response.raise_for_status()

            branches = response.json()
            if not isinstance(branches, list):
                return {"cleaned_up": 0}

            # Find branches that match our council pattern but aren't the current one
            council_prefix = f"{self.council_code}-"
            current_branch = self.branch_name
            cleanup_count = 0

            for branch in branches:
                ref = branch["ref"]
                branch_name = ref.replace("refs/heads/", "")

                # Skip if not our council's branch or if it's the current branch
                if (
                    not branch_name.startswith(council_prefix)
                    or branch_name == current_branch
                ):
                    continue

                # Delete the old branch
                try:
                    delete_response = self.session.delete(
                        f"{self.api_base_url}/git/refs/heads/{branch_name}"
                    )
                    if delete_response.status_code in [200, 204]:
                        cleanup_count += 1
                except:
                    continue  # Skip if deletion fails

            return {"cleaned_up": cleanup_count}

        except Exception as e:
            return {"error": str(e), "cleaned_up": 0}

    def _delete_existing_data_if_needed(self):
        """Get information about existing data that will be replaced"""
        try:
            # Get the default branch to check for existing files
            repo_response = self.session.get(f"{self.api_base_url}")
            repo_response.raise_for_status()
            default_branch = repo_response.json()["default_branch"]

            # Check if council folder exists and has data for this scraper type
            folder_path = f"{self.council_code}/{self.scraper_object_type}"
            self._existing_files = []

            def collect_files(path: str) -> None:
                """Recursively collect all files in the scraper type folder"""
                try:
                    contents_url = f"{self.api_base_url}/contents/{path}"
                    contents_response = self.session.get(
                        contents_url, params={"ref": default_branch}
                    )
                    if contents_response.status_code == 200:
                        items = contents_response.json()
                        if not isinstance(items, list):
                            items = [items]

                        for item in items:
                            if item["type"] == "file":
                                self._existing_files.append(item["path"])
                            elif item["type"] == "dir":
                                collect_files(item["path"])
                except requests.exceptions.HTTPError:
                    pass

            collect_files(folder_path)

            return {
                "existing_files_found": len(self._existing_files),
                "files_to_replace": self._existing_files,
            }

        except Exception as e:
            # If we can't check, proceed anyway
            self._existing_files = []
            return {"error": str(e), "will_proceed": True}

    def write(self, filename: Path, content: str) -> None:
        """Stage a UTF-8 text file for writing in this session."""
        self._assert_open()
        if filename.is_absolute():
            raise ValueError("filename must be relative")

        # Prefix with council code for folder isolation
        full_path = Path(self.council_code) / filename
        key = str(full_path).replace("\\", "/")  # Normalize path separators
        self._staged[key] = content.encode("utf-8")

    def write_bytes(self, filename: Path, content: bytes) -> None:
        """Stage a binary file for writing in this session."""
        self._assert_open()
        if filename.is_absolute():
            raise ValueError("filename must be relative")

        # Prefix with council code for folder isolation
        full_path = Path(self.council_code) / filename
        key = str(full_path).replace("\\", "/")  # Normalize path separators
        self._staged[key] = content

    def touch(self, filename: Path) -> None:
        """Stage an empty file for creation in this session."""
        self.write_bytes(filename, b"")

    def open(self, filename: Path, mode: str = "r") -> Union[str, bytes]:
        """Read a file within this session context."""
        self._assert_open()
        if filename.is_absolute():
            raise ValueError("filename must be relative")

        # Check staged files first
        full_path = Path(self.council_code) / filename
        key = str(full_path).replace("\\", "/")

        if key in self._staged:
            content = self._staged[key]
            if mode == "r":
                return content.decode("utf-8")
            else:
                return content

        # Try to read from repository
        try:
            contents_url = f"{self.api_base_url}/contents/{key}"
            response = self.session.get(contents_url)
            if response.status_code == 200:
                import base64

                file_data = response.json()
                content = base64.b64decode(file_data["content"])
                if mode == "r":
                    return content.decode("utf-8")
                else:
                    return content
            else:
                raise FileNotFoundError(f"File not found: {filename}")
        except requests.exceptions.HTTPError:
            raise FileNotFoundError(f"File not found: {filename}")

    def _assert_open(self):
        """Assert that the session is still open."""
        if self._closed:
            raise RuntimeError("Storage session is closed")

    def _commit_files(self, commit_message: str) -> Dict[str, Any]:
        """Commit all staged files to the GitHub repository with clean-slate approach."""
        if not self._staged:
            return {"skipped": True, "reason": "no files to commit"}

        self._ensure_branch_exists()

        # Get current tree SHA
        current_commit_url = f"{self.api_base_url}/git/commits/{self._base_sha}"
        current_commit_response = self.session.get(current_commit_url)
        current_commit_response.raise_for_status()
        current_tree_sha = current_commit_response.json()["tree"]["sha"]

        # Create blobs for all staged files with retry for network issues
        blobs = {}
        for file_path, content in self._staged.items():
            import base64

            blob_data = {
                "content": base64.b64encode(content).decode("ascii"),
                "encoding": "base64",
            }

            # Retry blob creation for network issues
            for retry_attempt in range(3):
                try:
                    blob_response = self.session.post(
                        f"{self.api_base_url}/git/blobs", json=blob_data
                    )
                    blob_response.raise_for_status()
                    blobs[file_path] = blob_response.json()["sha"]
                    break
                except (
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                ) as e:
                    if retry_attempt < 2:
                        time.sleep(2**retry_attempt)
                        continue
                    else:
                        raise

        # Create new tree with staged files and deletions
        tree_items = []

        # Add all staged files
        for file_path, blob_sha in blobs.items():
            tree_items.append(
                {"path": file_path, "mode": "100644", "type": "blob", "sha": blob_sha}
            )

        # Delete existing files for this scraper type that aren't in staged files
        scraper_folder_prefix = f"{self.council_code}/{self.scraper_object_type}/"
        existing_files_to_delete = getattr(self, "_existing_files", [])

        for existing_file_path in existing_files_to_delete:
            # Only delete files that belong to this scraper type and aren't being updated
            if (
                existing_file_path.startswith(scraper_folder_prefix)
                and existing_file_path not in self._staged
            ):
                tree_items.append(
                    {
                        "path": existing_file_path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": None,
                    }
                )

        tree_data = {"base_tree": current_tree_sha, "tree": tree_items}

        # Create tree with retry
        for retry_attempt in range(3):
            try:
                tree_response = self.session.post(
                    f"{self.api_base_url}/git/trees", json=tree_data
                )
                tree_response.raise_for_status()
                new_tree_sha = tree_response.json()["sha"]
                break
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                if retry_attempt < 2:
                    time.sleep(2**retry_attempt)
                    continue
                else:
                    raise

        # Create commit
        commit_data = {
            "message": commit_message,
            "tree": new_tree_sha,
            "parents": [self._base_sha],
        }

        for retry_attempt in range(3):
            try:
                commit_response = self.session.post(
                    f"{self.api_base_url}/git/commits", json=commit_data
                )
                commit_response.raise_for_status()
                new_commit_sha = commit_response.json()["sha"]
                break
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                if retry_attempt < 2:
                    time.sleep(2**retry_attempt)
                    continue
                else:
                    raise

        # Update branch reference
        update_ref_url = f"{self.api_base_url}/git/refs/heads/{self.branch_name}"
        update_ref_data = {"sha": new_commit_sha, "force": False}

        for retry_attempt in range(3):
            try:
                update_response = self.session.patch(
                    update_ref_url, json=update_ref_data
                )
                update_response.raise_for_status()
                break
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                if retry_attempt < 2:
                    time.sleep(2**retry_attempt)
                    continue
                else:
                    raise

        # Calculate deletion count
        deleted_count = 0
        existing_files_to_delete = getattr(self, "_existing_files", [])
        scraper_folder_prefix = f"{self.council_code}/{self.scraper_object_type}/"

        for existing_file_path in existing_files_to_delete:
            if (
                existing_file_path.startswith(scraper_folder_prefix)
                and existing_file_path not in self._staged
            ):
                deleted_count += 1

        return {
            "files_committed": len(self._staged),
            "files_deleted": deleted_count,
            "commit_sha": new_commit_sha,
            "branch": self.branch_name,
        }


class GitHubStorage(BaseStorage):
    """
    GitHub storage backend with git-based operations and per-run branching.

    This storage backend implements the BaseStorage interface using GitHub
    repositories. It provides:

    - **Git-based storage**: All files are stored in git repositories
    - **Council isolation**: Each council gets its own folder in the repository
    - **Per-run branching**: Operations happen on unique run-specific branches
    - **Atomic commits**: Session operations are committed atomically
    - **Race condition handling**: Unique branch names prevent conflicts
    - **GitHub integration**: Native integration with GitHub API
    - **Pull request workflow**: Configurable PR creation and auto-merge behavior

    Architecture:
    - Single repository shared across all councils
    - Council data is isolated in folders: {council-code}/
    - Run-specific branches follow the pattern: {council-code}-{YYYY-MM-DD}-{run-id}
    - Files are organized under scraper type subdirectories

    Repository Structure:
        main/
        ├── council1/
        │   └── Councillors/
        │       ├── json/
        │       │   └── councillor1.json
        │       └── raw/
        │           └── councillor1.html
        └── council2/
            └── Councillors/
                ├── json/
                └── raw/

    Branch Structure:
        main                           # Main branch for merged data
        council1-2024-01-15-abc123     # Unique run branch
        council2-2024-01-15-def456     # Another council's run branch

    Pull Request Workflow:
    - When auto_merge=False: PR is created but left open for manual review
    - When auto_merge=True (default): PR is created and immediately merged
    - Branch cleanup only occurs after successful merge
    - Failed PR creation or merge leaves branches for manual intervention

    Environment Requirements:
    - GITHUB_REPOSITORY_URL: Repository URL in format 'owner/repo' or full GitHub URL
    - GITHUB_TOKEN: Personal access token or GitHub App token with repo permissions

    GitHub Token Permissions Required:
    - Contents: read/write (for file operations)
    - Metadata: read (for repository information)
    - Pull requests: write (for creating and merging PRs)

    Examples:
        # Basic usage with manual review (default behavior)
        storage = GitHubStorage(council_code="ABC123")
        with storage.session("Scrape councillors") as session:
            session.write(Path("Councillors/json/councillor1.json"), json_data)
            # PR created but left open for manual review

        # Auto-merge workflow
        storage = GitHubStorage(
            council_code="ABC123",
            auto_merge=True
        )
        with storage.session("Scrape councillors") as session:
            session.write(Path("Councillors/json/councillor1.json"), json_data)
            # PR created and auto-merged at session end

        # Manual repository specification
        storage = GitHubStorage(
            council_code="ABC123",
            repository_url="myorg/council-data",
            github_token="ghp_...",
            auto_merge=True
        )
    """

    def __init__(
        self,
        council_code: str,
        scraper_object_type: str = "Data",
        repository_url: Optional[str] = None,
        github_token: Optional[str] = None,
        auto_merge: bool = True,
    ):
        """
        Initialize GitHub storage backend.

        Args:
            council_code: Unique identifier for the council
            scraper_object_type: Type of objects being scraped (default: "Data")
            repository_url: GitHub repository URL (owner/repo format or full URL)
            github_token: GitHub authentication token
            auto_merge: If True, automatically merge PRs after creation (default: False)
                       If False, create PRs but leave them open for manual review

        Raises:
            ValueError: If repository_url or github_token are not provided
        """
        super().__init__(council_code)
        self.scraper_object_type: str = scraper_object_type

        # Get repository URL from parameter or environment
        self.repository_url: Optional[str] = repository_url or os.environ.get(
            "GITHUB_REPOSITORY_URL"
        )
        if not self.repository_url:
            raise ValueError(
                "GitHub repository URL not provided. Set GITHUB_REPOSITORY_URL environment "
                "variable or pass repository_url parameter."
            )

        # Get GitHub token from parameter or environment
        self.github_token: Optional[str] = github_token or os.environ.get(
            "GITHUB_TOKEN"
        )
        if not self.github_token:
            raise ValueError(
                "GitHub token not provided. Set GITHUB_TOKEN environment variable "
                "or pass github_token parameter."
            )

        self._active: Optional[_GitHubSession] = None
        self.auto_merge: bool = auto_merge

    def _start_session(self, **kwargs) -> StorageSession:
        """
        Create a new GitHub session for this council.

        This method handles all preparation:
        - Creates a unique branch for this run
        - Checks for existing data that may need cleanup
        - Returns a session ready for file operations

        Args:
            **kwargs: Additional parameters:
                - scraper_object_type: Override the scraper type for file organization
                - run_id: Override the run ID for branch naming

        Returns:
            _GitHubSession: A new session for GitHub operations

        Raises:
            RuntimeError: If a session is already active
            ValueError: If repository URL or token is invalid
            requests.exceptions.HTTPError: If GitHub API operations fail
        """
        if self._active is not None:
            raise RuntimeError(
                "A session is already active on this GitHubStorage instance."
            )

        scraper_type = kwargs.get("scraper_object_type", self.scraper_object_type)
        run_id = kwargs.get("run_id")

        session = _GitHubSession(
            repository_url=self.repository_url or "",
            github_token=self.github_token or "",
            council_code=self.council_code,
            scraper_object_type=scraper_type,
            run_id=run_id,
            storage_backend=self,
        )

        # Check for existing data (for logging purposes)
        delete_result = session._delete_existing_data_if_needed()
        if delete_result.get("existing_files_found", 0) > 0:
            session._preparation_result = delete_result

        self._active = session
        return session

    def _end_session(self, session: StorageSession, commit_message: str, **kwargs):
        """
        Commit all staged changes and perform GitHub finalization with PR workflow.

        This method handles the complete workflow:
        1. Commits all staged files atomically to the run branch
        2. Creates a pull request from the run branch to main branch
        3. Optionally auto-merges the PR (based on auto_merge setting)
        4. Cleans up the run branch after successful merge (auto-merge only)
        5. Cleans up any old branches from previous runs (auto-merge only)

        Manual review behavior (auto_merge=False, default):
        - PR is created but left open for manual review
        - Branch is preserved for the PR to remain valid
        - No cleanup of old branches occurs

        Auto-merge behavior (auto_merge=True):
        - PR is created and immediately merged using squash method
        - Branch is cleaned up after successful merge
        - Old branches from previous runs are also cleaned up

        Args:
            session: The session to commit (must be from this storage instance)
            commit_message: Description of the changes (must be non-empty)
            **kwargs: Additional parameters:
                - run_log: Optional run log for logbook updates
                - skip_merge: If True, skip PR creation and cleanup (default: False)
                - max_merge_retries: Maximum retries for merge conflicts (default: 3)

        Returns:
            dict: Complete workflow results including commit, PR creation, and cleanup info
                - For auto_merge=False (default): includes PR URL but no merge/cleanup results
                - For auto_merge=True: includes merge and cleanup results

        Raises:
            ValueError: If commit_message is empty
            RuntimeError: If session is invalid or doesn't belong to this instance
            requests.exceptions.HTTPError: If GitHub API operations fail
        """
        # Handle case where session has already been finalized
        if not isinstance(session, _GitHubSession):
            raise RuntimeError("Unknown session type for this GitHubStorage.")

        if session is not self._active:
            # Session already finalized or not from this storage instance
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

            # If no changes were detected, skip finalization but clean up properly
            if commit_result.get("skipped"):
                return commit_result

            # Perform GitHub-specific finalization with automatic merge and cleanup
            finalization_result = commit_result.copy()

            # Skip merge and cleanup if explicitly requested (for testing)
            if kwargs.get("skip_merge", False):
                return finalization_result

            # Step 1: Create PR and optionally merge to main branch
            pr_result = self._create_pull_request(
                session, max_retries=kwargs.get("max_merge_retries", 3)
            )
            finalization_result["pull_request"] = pr_result

            # Step 2: Clean up current branch (only if PR was merged)
            if pr_result.get("success") and pr_result.get("merged", False):
                cleanup_result = self._delete_branch(session)
                finalization_result["branch_cleanup"] = cleanup_result

                # Step 3: Clean up old branches from previous runs
                old_branches_cleanup = session._cleanup_old_branches()
                finalization_result["old_branches_cleanup"] = old_branches_cleanup
            elif pr_result.get("success") and not pr_result.get("merged", True):
                # PR created but not merged - keep branch for review
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
            # Always clean up session state
            self._reset_session_state(session)

    def _reset_session_state(self, session: Optional[StorageSession]) -> None:
        """Reset session state after an error occurs."""
        if isinstance(session, _GitHubSession):
            session._closed = True
        self._active = None

    def _create_pull_request(
        self, session: _GitHubSession, max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Create a pull request and optionally merge it to the main branch.

        Args:
            session: The active session
            max_retries: Maximum number of retry attempts for merge conflicts

        Returns:
            dict: PR creation and merge result information
        """
        for attempt in range(max_retries + 1):
            try:
                # Get repository info to find default branch
                repo_url = f"{session.api_base_url}"
                repo_response = session.session.get(repo_url)
                repo_response.raise_for_status()
                default_branch = repo_response.json()["default_branch"]

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
                    # Merge pull request
                    merge_data = {
                        "commit_title": f"Merge {session.council_code} data ({session.today})",
                        "merge_method": "squash",
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
                    # Merge conflict during auto-merge, wait and retry
                    time.sleep(2**attempt)  # Exponential backoff
                    continue
                else:
                    return {"success": False, "error": str(e), "attempt": attempt + 1}

        return {
            "success": False,
            "error": "Maximum merge retries exceeded",
            "attempts": max_retries + 1,
        }

    def _delete_branch(self, session: _GitHubSession) -> Dict[str, Any]:
        """
        Delete the session's branch after successful merge.

        Args:
            session: The active session

        Returns:
            dict: Branch deletion result
        """
        try:
            delete_url = f"{session.api_base_url}/git/refs/heads/{session.branch_name}"
            response = session.session.delete(delete_url)
            response.raise_for_status()

            return {"success": True, "branch": session.branch_name}

        except requests.exceptions.HTTPError as e:
            return {"success": False, "error": str(e), "branch": session.branch_name}
