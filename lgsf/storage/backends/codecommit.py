from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Union

import boto3
from botocore.exceptions import ClientError

from lgsf.storage.backends.base import BaseStorage, StorageSession


class _CodeCommitSession(StorageSession):
    """
    CodeCommit session implementation for AWS CodeCommit repositories.

    This session implementation stages all file operations in memory and creates
    a single commit with all changes when the session is committed. It provides:

    - **Git-based operations**: All files are committed to a git repository
    - **Daily branches**: Operations happen on date-based branches
    - **Atomic commits**: All staged files are committed together
    - **Repository management**: Automatically creates repos and branches as needed

    The session works with AWS CodeCommit's API to provide git-like semantics
    while maintaining the storage session interface.

    Args:
        repository_name: Name of the CodeCommit repository
        codecommit_client: Boto3 CodeCommit client
        council_code: Council identifier for isolation
        scraper_object_type: Type of scraper (e.g., "Councillors")
    """

    def __init__(
        self,
        repository_name: str,
        codecommit_client,
        council_code: str,
        scraper_object_type: str = "Data",
        storage_backend=None,
    ):
        self.repository_name = repository_name
        self.codecommit_client = codecommit_client
        self.council_code = council_code
        self.scraper_object_type = scraper_object_type
        self.storage_backend = storage_backend

        self._staged: Dict[str, bytes] = {}
        self._closed = False

        # Branch and commit management
        self.today = datetime.datetime.now().strftime("%Y-%m-%d")
        self._branch_head = ""

        # Repository and branch will be ensured when needed

    @property
    def branch_name(self) -> str:
        """Returns today's branch name in format: {council}-{date}"""
        return f"{self.council_code}-{self.today}"

    @property
    def branch_head(self) -> str:
        """Returns the current HEAD commit of today's branch"""
        if not self._branch_head:
            try:
                branch_info = self.codecommit_client.get_branch(
                    repositoryName=self.repository_name,
                    branchName=self.branch_name
                )
                self._branch_head = branch_info["branch"]["commitId"]
            except self.codecommit_client.exceptions.BranchDoesNotExistException:
                self._branch_head = self._create_branch()
        return self._branch_head

    @branch_head.setter
    def branch_head(self, commit_id: str):
        self._branch_head = commit_id

    def _ensure_repository_exists(self):
        """Ensure the CodeCommit repository exists, create if needed"""
        try:
            self.codecommit_client.get_repository(repositoryName=self.repository_name)
        except ClientError as error:
            if error.response["Error"]["Code"] == "RepositoryDoesNotExistException":
                self._create_repository()
            else:
                raise

    def _delete_existing_data_if_needed(self):
        """Delete existing data for this scraper type if any exists"""
        try:
            _, file_paths = self.storage_backend._get_files(f"{self.scraper_object_type}", self.branch_name)

            if not file_paths:
                return {"deleted": 0, "message": "No existing data to delete"}

            # Delete files in batches
            batch_size = 100
            total_deleted = 0
            commit_id = self.branch_head

            while file_paths:
                batch_files = file_paths[:batch_size]
                file_paths = file_paths[batch_size:]

                delete_files = [{"filePath": fp} for fp in batch_files]
                message = f"Deleting batch of {len(delete_files)} files from {self.scraper_object_type}"

                commit_info = self.codecommit_client.create_commit(
                    repositoryName=self.repository_name,
                    branchName=self.branch_name,
                    parentCommitId=commit_id,
                    commitMessage=message,
                    deleteFiles=delete_files,
                )

                commit_id = commit_info["commitId"]
                total_deleted += len(delete_files)

            # Update branch head
            self.branch_head = commit_id
            return {"deleted": total_deleted, "commit_id": commit_id}

        except self.codecommit_client.exceptions.BranchDoesNotExistException:
            return {"deleted": 0, "message": "Branch does not exist yet"}

    def _create_repository(self):
        """Create a new CodeCommit repository"""
        try:
            self.codecommit_client.create_repository(repositoryName=self.repository_name)
        except ClientError as error:
            if error.response["Error"]["Code"] != "RepositoryNameExistsException":
                raise

    def _create_branch(self) -> str:
        """
        Create today's branch from main branch.
        Returns the commit ID of the new branch's HEAD.
        """
        try:
            # Get main branch info
            main_info = self.codecommit_client.get_branch(
                repositoryName=self.repository_name,
                branchName="main"
            )
            commit_id = main_info["branch"]["commitId"]
        except self.codecommit_client.exceptions.BranchDoesNotExistException:
            # No main branch yet - this is a new repository
            # We'll create the branch with the first commit
            return ""

        # Create new branch pointing to main's HEAD
        self.codecommit_client.create_branch(
            repositoryName=self.repository_name,
            branchName=self.branch_name,
            commitId=commit_id,
        )
        return commit_id

    # --- StorageSession API ---
    def write(self, filename: Path, content: str) -> None:
        self._assert_open()
        key = self._key(filename)
        self._staged[key] = content.encode("utf-8")

    def write_bytes(self, filename: Path, content: bytes) -> None:
        self._assert_open()
        key = self._key(filename)
        self._staged[key] = content

    def touch(self, filename: Path) -> None:
        self._assert_open()
        key = self._key(filename)
        self._staged[key] = b""

    def open(self, filename: Path, mode: Literal["r", "rb"] = "r") -> Union[str, bytes]:
        self._assert_open()
        key = self._key(filename)

        # Check staged content first
        if key in self._staged:
            data = self._staged[key]
            return data if mode == "rb" else data.decode("utf-8")

        # Try to read from repository
        try:
            file_response = self.codecommit_client.get_file(
                repositoryName=self.repository_name,
                commitSpecifier=self.branch_name,
                filePath=key
            )
            data = file_response["fileContent"]
            return data if mode == "rb" else data.decode("utf-8")
        except self.codecommit_client.exceptions.FileDoesNotExistException:
            raise FileNotFoundError(f"File not found: {key}")

    # --- Internal methods ---
    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("Session is closed.")

    def _key(self, p: Path) -> str:
        """Convert a Path to a relative string key suitable for CodeCommit"""
        if p.is_absolute():
            raise ValueError(f"Absolute paths not allowed: {p}")

        rel = p.as_posix().lstrip("/")
        if not rel:
            raise ValueError("Empty path not allowed")

        # Check for path traversal attempts
        if ".." in p.parts:
            raise ValueError(f"Path traversal not allowed: {p}")

        # Prefix with scraper object type for organization
        return f"{self.scraper_object_type}/{rel}"

    def _consume_staged(self) -> Dict[str, bytes]:
        """Consume and return all staged files, without closing the session"""
        self._assert_open()
        staged = self._staged
        self._staged = {}
        return staged

    def _commit_files(self, commit_message: str) -> Dict[str, Any]:
        """
        Commit all staged files to the repository in batches if needed.

        CodeCommit has a limit of 100 files per commit, so we batch commits
        when there are more files.

        Returns commit information from CodeCommit.
        """
        staged = self._consume_staged()
        if not staged:
            return {"skipped": True, "reason": "no changes"}

        # CodeCommit limit: 100 files per commit
        max_files_per_commit = 100
        staged_items = list(staged.items())
        total_files = len(staged_items)
        commits = []

        # Process files in batches
        for i in range(0, total_files, max_files_per_commit):
            batch = staged_items[i:i + max_files_per_commit]
            batch_num = (i // max_files_per_commit) + 1
            total_batches = (total_files + max_files_per_commit - 1) // max_files_per_commit

            # Prepare put_files for this batch
            put_files = []
            for file_path, content in batch:
                put_files.append({
                    "filePath": file_path,
                    "fileContent": content
                })

            # Create batch-specific commit message
            if total_batches > 1:
                batch_message = f"{commit_message} (batch {batch_num}/{total_batches})"
            else:
                batch_message = commit_message

            try:
                commit_params = {
                    "repositoryName": self.repository_name,
                    "branchName": self.branch_name,
                    "commitMessage": batch_message,
                    "putFiles": put_files,
                }
                if self.branch_head:
                    commit_params["parentCommitId"] = self.branch_head

                commit_info = self.codecommit_client.create_commit(**commit_params)
            except self.codecommit_client.exceptions.ParentCommitIdOutdatedException:
                # Branch head has moved, refresh and retry
                self._branch_head = ""  # Reset cached head
                retry_params = {
                    "repositoryName": self.repository_name,
                    "branchName": self.branch_name,
                    "commitMessage": batch_message,
                    "putFiles": put_files,
                }
                if self.branch_head:
                    retry_params["parentCommitId"] = self.branch_head

                commit_info = self.codecommit_client.create_commit(**retry_params)
            except self.codecommit_client.exceptions.NoChangeException:
                # No changes detected for this batch, skip it
                continue

            # Update branch head for next batch
            self.branch_head = commit_info["commitId"]
            commits.append({
                "commit_id": commit_info["commitId"],
                "files_count": len(put_files),
                "batch": batch_num
            })

        # Close the session now that commits are complete
        self._closed = True

        if not commits:
            # No commits were made (all batches had no changes)
            # Don't close the session here since we need to return it properly
            return {"skipped": True, "reason": "no changes detected"}

        result = {
            "applied": total_files,
            "commit_id": commits[-1]["commit_id"] if commits else None,
            "repository": self.repository_name,
            "branch": self.branch_name,
            "files": list(staged.keys()),
            "batches": commits,
            "total_batches": len(commits)
        }

        return result


class CodeCommitStorage(BaseStorage):
    """
    AWS CodeCommit storage backend with git-based operations and daily branching.

    This storage backend implements the BaseStorage interface using AWS CodeCommit
    repositories. It provides:

    - **Git-based storage**: All files are stored in git repositories
    - **Council isolation**: Each council gets its own repository
    - **Daily branching**: Operations happen on date-specific branches
    - **Atomic commits**: Session operations are committed atomically
    - **AWS integration**: Native integration with AWS CodeCommit service

    Architecture:
    - Each council has its own CodeCommit repository
    - Daily branches follow the pattern: {council-code}-{YYYY-MM-DD}
    - Files are organized under scraper type subdirectories
    - Sessions create single commits with all changes

    Repository Structure:
        main/
        └── {ScraperType}/
            ├── json/
            │   └── councillor1.json
            └── raw/
                └── councillor1.html

    Branch Structure:
        main                    # Main branch for merged data
        council-2024-01-15     # Daily working branch
        council-2024-01-16     # Next day's working branch

    AWS Requirements:
    - Valid AWS credentials configured
    - CodeCommit service permissions:
      - codecommit:CreateRepository
      - codecommit:GetRepository
      - codecommit:CreateBranch
      - codecommit:GetBranch
      - codecommit:CreateCommit
      - codecommit:GetFile
      - codecommit:MergeBranchesBySquash (for advanced workflows)

    Examples:
        # Basic usage
        storage = CodeCommitStorage(council_code="ABC123")
        with storage.session("Scrape councillors") as session:
            session.write(Path("json/councillor1.json"), json_data)
            session.write(Path("raw/councillor1.html"), html_content)

        # Reading existing data
        with storage.session("Process data") as session:
            data = session.open(Path("json/councillor1.json"))
            # ... process data ...
            session.write(Path("processed/result.json"), processed_data)
    """

    def __init__(self, council_code: str, scraper_object_type: str = "Data"):
        super().__init__(council_code)
        self.scraper_object_type = scraper_object_type
        self.repository_name = council_code  # Repository name matches council code
        self.codecommit_client = boto3.client("codecommit")
        self._active: Optional[_CodeCommitSession] = None

    def _start_session(self, **kwargs) -> StorageSession:
        """
        Create a new CodeCommit session for this council.

        This method handles all preparation:
        - Creates repository and branch if needed
        - Deletes existing data
        - Returns a session ready for file operations

        Args:
            **kwargs: Additional parameters:
                - scraper_object_type: Override the scraper type for file organization

        Returns:
            _CodeCommitSession: A new session for CodeCommit operations

        Raises:
            RuntimeError: If a session is already active
            ClientError: If AWS CodeCommit operations fail
        """
        if self._active is not None:
            raise RuntimeError(
                "A session is already active on this CodeCommitStorage instance."
            )

        scraper_type = kwargs.get("scraper_object_type", self.scraper_object_type)

        session = _CodeCommitSession(
            repository_name=self.repository_name,
            codecommit_client=self.codecommit_client,
            council_code=self.council_code,
            scraper_object_type=scraper_type,
            storage_backend=self
        )

        # Ensure repository exists
        session._ensure_repository_exists()

        # Delete existing data for this scraper type
        delete_result = session._delete_existing_data_if_needed()
        if delete_result["deleted"] > 0:
            # Store result for logging in the scraper
            session._preparation_result = delete_result

        self._active = session
        return session

    def _end_session(self, session: StorageSession, commit_message: str, **kwargs):
        """
        Commit all staged changes and perform CodeCommit finalization.

        This method handles:
        - Committing all staged files atomically
        - Updating logbook if run_log is provided
        - Merging to main branch
        - Deleting daily branch
        - Cleaning up session state

        Args:
            session: The session to commit (must be from this storage instance)
            commit_message: Description of the changes (must be non-empty)
            **kwargs: Additional parameters:
                - run_log: Optional run log for logbook updates

        Returns:
            dict: Commit information with finalization details

        Raises:
            ValueError: If commit_message is empty
            RuntimeError: If session is invalid or doesn't belong to this instance
            ClientError: If AWS CodeCommit operations fail
        """
        if not isinstance(session, _CodeCommitSession) or session is not self._active:
            raise RuntimeError(
                "Unknown or inactive session for this CodeCommitStorage."
            )

        if not commit_message or not commit_message.strip():
            raise ValueError("commit_message cannot be empty")

        try:
            # Commit the staged files
            commit_result = session._commit_files(commit_message.strip())

            # If no changes were detected, skip finalization but clean up properly
            if commit_result.get("skipped"):
                if commit_result.get("reason") == "no changes detected":
                    # Still need to close the session properly
                    session._closed = True
                    return {"skipped": True, "reason": "no changes detected"}
                else:
                    # Other skip reasons (like empty staging)
                    return commit_result

            # Perform CodeCommit-specific finalization
            run_log = kwargs.get('run_log')
            finalization_result = {}

            if run_log:
                # Update logbook
                try:
                    if hasattr(run_log, 'log') and not getattr(run_log, 'log', None):
                        # Console log will be set by the caller
                        pass

                    if not hasattr(run_log, 'finished') or not run_log.finished:
                        run_log.finish()

                    logbook_result = self.create_or_update_logbook(run_log.as_json)
                    finalization_result['logbook'] = logbook_result
                except Exception as e:
                    finalization_result['logbook_error'] = str(e)

            # Merge to main branch
            try:
                merge_result = self.merge_to_main("Daily scrape completed")
                finalization_result['merge'] = merge_result

                # Delete daily branch
                delete_result = self.delete_daily_branch()
                finalization_result['branch_cleanup'] = delete_result

            except Exception as e:
                finalization_result['finalization_error'] = str(e)

            # Combine results
            result = commit_result.copy()
            result['finalization'] = finalization_result

            return result

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

    def delete_existing_data(self, scraper_object_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Delete all existing data for this council's scraper type.

        This method provides a way to clean up existing data before a fresh scrape,
        similar to the delete_data_if_exists method from the original CodeCommitMixin.

        Args:
            scraper_object_type: Override the scraper type for deletion.
                               Defaults to instance's scraper_object_type.

        Returns:
            dict: Information about the deletion operation:
                - deleted: Number of files deleted
                - commit_id: The commit ID if deletions were made
                - skipped: True if no files were found to delete

        Raises:
            ClientError: If AWS CodeCommit operations fail
        """
        scraper_type = scraper_object_type or self.scraper_object_type

        try:
            # Create a temporary session to get today's branch
            temp_session = _CodeCommitSession(
                repository_name=self.repository_name,
                codecommit_client=self.codecommit_client,
                council_code=self.council_code,
                scraper_object_type=scraper_type
            )

            # Get all files in the scraper directory
            _, file_paths = self._get_files(f"{scraper_type}", temp_session.branch_name)

            if not file_paths:
                return {"skipped": True, "reason": "no files to delete"}

            # Delete files in batches (CodeCommit has limits)
            batch_size = 100
            total_deleted = 0
            commit_id = temp_session.branch_head

            while file_paths:
                batch_files = file_paths[:batch_size]
                file_paths = file_paths[batch_size:]

                delete_files = [{"filePath": fp} for fp in batch_files]
                message = f"Deleting batch of {len(delete_files)} files from {scraper_type}"

                commit_info = self.codecommit_client.create_commit(
                    repositoryName=self.repository_name,
                    branchName=temp_session.branch_name,
                    parentCommitId=commit_id,
                    commitMessage=message,
                    deleteFiles=delete_files,
                )

                commit_id = commit_info["commitId"]
                total_deleted += len(delete_files)

            return {
                "deleted": total_deleted,
                "commit_id": commit_id,
                "repository": self.repository_name,
                "branch": temp_session.branch_name
            }

        except self.codecommit_client.exceptions.BranchDoesNotExistException:
            return {"skipped": True, "reason": "branch does not exist"}

    def _get_files(self, folder_path: str, branch_name: str):
        """
        Recursively get all files in a folder from the repository.

        Returns:
            tuple: (subfolder_paths, file_paths) lists
        """
        subfolder_paths = []
        file_paths = []

        try:
            folder = self.codecommit_client.get_folder(
                repositoryName=self.repository_name,
                commitSpecifier=branch_name,
                folderPath=folder_path,
            )

            for subfolder in folder["subFolders"]:
                subfolder_paths.append(subfolder["absolutePath"])

            for file in folder["files"]:
                file_paths.append(file["absolutePath"])

            # Recursively get files from subfolders
            for subfolder_path in subfolder_paths[:]:  # Copy list to avoid modification issues
                sf_paths, f_paths = self._get_files(subfolder_path, branch_name)
                subfolder_paths.extend(sf_paths)
                file_paths.extend(f_paths)

        except (self.codecommit_client.exceptions.FolderDoesNotExistException,
                self.codecommit_client.exceptions.CommitDoesNotExistException):
            pass  # Folder doesn't exist or commit doesn't exist, return empty lists

        return subfolder_paths, file_paths

    def merge_to_main(self, commit_message: Optional[str] = None) -> Dict[str, Any]:
        """
        Merge today's branch into the main branch using squash merge.

        This method merges all commits from today's daily branch into the main
        branch as a single squashed commit, similar to the original CodeCommitMixin
        behavior.

        Args:
            commit_message: Custom commit message for the merge. If not provided,
                          a default message with today's date will be used.

        Returns:
            dict: Merge information containing:
                - commit_id: The merge commit ID on main
                - source_branch: The branch that was merged
                - target_branch: Always "main"
                - commit_message: The merge commit message

        Raises:
            ClientError: If the merge operation fails
        """
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        source_branch = f"{self.council_code}-{today}"

        if not commit_message:
            commit_message = f"{self.scraper_object_type} - scraped on {today}"

        try:
            merge_info = self.codecommit_client.merge_branches_by_squash(
                repositoryName=self.repository_name,
                sourceCommitSpecifier=source_branch,
                destinationCommitSpecifier="main",
                commitMessage=commit_message,
            )

            return {
                "commit_id": merge_info["commitId"],
                "source_branch": source_branch,
                "target_branch": "main",
                "commit_message": commit_message,
                "repository": self.repository_name
            }

        except self.codecommit_client.exceptions.BranchDoesNotExistException as e:
            raise RuntimeError(f"Cannot merge: {e}")

    def delete_daily_branch(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Delete a daily branch after it has been merged.

        Args:
            date: Date string in YYYY-MM-DD format. If not provided, uses today's date.

        Returns:
            dict: Information about the deleted branch

        Raises:
            ClientError: If the deletion fails
        """
        if not date:
            date = datetime.datetime.now().strftime("%Y-%m-%d")

        branch_name = f"{self.council_code}-{date}"

        try:
            self.codecommit_client.delete_branch(
                repositoryName=self.repository_name,
                branchName=branch_name
            )

            return {
                "deleted": True,
                "branch_name": branch_name,
                "repository": self.repository_name
            }

        except self.codecommit_client.exceptions.BranchDoesNotExistException:
            return {"deleted": False, "reason": "branch does not exist"}

    def create_or_update_logbook(self, run_log_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create or update a logbook file with run information.

        This maintains a JSON file with the latest run information, similar to
        the original CodeCommitMixin's logbook functionality.

        Args:
            run_log_data: Dictionary containing run information to log

        Returns:
            dict: Information about the logbook update

        Raises:
            ClientError: If CodeCommit operations fail
        """
        log_file_path = f"{self.scraper_object_type}/logbook.json"

        # Try to get existing logbook
        try:
            logbook_response = self.codecommit_client.get_file(
                repositoryName=self.repository_name,
                filePath=log_file_path
            )
            logbook = json.loads(logbook_response["fileContent"].decode("utf-8"))
        except self.codecommit_client.exceptions.FileDoesNotExistException:
            # Create new logbook
            logbook = {
                "name": self.council_code,
                "runs": []
            }

        # Add new run, keeping only last 20 entries
        if len(logbook["runs"]) >= 20:
            logbook["runs"].pop(0)

        logbook["runs"].append(run_log_data)

        # Get today's branch info for commit
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        branch_name = f"{self.council_code}-{today}"

        try:
            branch_info = self.codecommit_client.get_branch(
                repositoryName=self.repository_name,
                branchName=branch_name
            )
            branch_head = branch_info["branch"]["commitId"]
        except self.codecommit_client.exceptions.BranchDoesNotExistException:
            # Create branch from main
            try:
                main_info = self.codecommit_client.get_branch(
                    repositoryName=self.repository_name,
                    branchName="main"
                )
                branch_head = main_info["branch"]["commitId"]

                self.codecommit_client.create_branch(
                    repositoryName=self.repository_name,
                    branchName=branch_name,
                    commitId=branch_head,
                )
            except self.codecommit_client.exceptions.BranchDoesNotExistException:
                branch_head = None  # New repository

        # Commit the logbook
        commit_info = self.codecommit_client.create_commit(
            repositoryName=self.repository_name,
            branchName=branch_name,
            parentCommitId=branch_head,
            commitMessage=f"Update logbook for {self.council_code}",
            putFiles=[{
                "filePath": log_file_path,
                "fileContent": json.dumps(logbook, indent=2).encode("utf-8")
            }]
        )

        return {
            "updated": True,
            "commit_id": commit_info["commitId"],
            "logbook_path": log_file_path,
            "runs_count": len(logbook["runs"])
        }
