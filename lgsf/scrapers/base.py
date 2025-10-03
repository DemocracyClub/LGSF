import abc
import datetime
import json
import traceback
from pathlib import Path

import boto3
import httpx
import requests
from botocore.exceptions import ClientError
from dateutil import parser

from ..aws_lambda.run_log import RunLog
from ..storage.backends import get_storage_backend
from .checks import ScraperChecker


class ScraperBase(metaclass=abc.ABCMeta):
    """
    Base class for a scraper. All scrapers should inherit from this.
    """

    disabled = False
    extra_headers = {}
    http_lib = "httpx"
    verify_requests = True
    timeout = 10

    def __init__(self, options, console):
        self.options = options
        self.console = console
        self.check()

        self.storage_backend = get_storage_backend(
            council_code=self.options["council"]
        )
        self.storage_session = self.storage_backend.start_session()
        if self.http_lib == "requests":
            self.http_client = requests.Session()
            self.http_client.verify = self.verify_requests
        else:
            self.http_client = httpx.Client(
                verify=self.verify_requests, follow_redirects=True
            )

    def get(self, url, extra_headers=None):
        """
        Wraps `requests.get`
        """

        if self.options.get("verbose"):
            self.console.log(f"Scraping from {url}")
        headers = {"User-Agent": "Scraper/DemocracyClub", "Accept": "*/*"}

        if extra_headers:
            headers.update(extra_headers)
        response = self.http_client.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return response

    def check(self):
        checker = ScraperChecker(self.__class__)
        checker.run_checks()

    def run_since(self, hours=24):
        now = datetime.datetime.now()
        delta = datetime.timedelta(hours=hours)
        last = self._get_last_run()
        if last and last > now - delta:
            return True
        return None

    def _file_name(self, name) -> Path:
        # Return just the filename for storage backend usage
        return Path(name)

    def _last_run_file_name(self) -> Path:
        return self._file_name("_last-run")

    def _error_file_name(self):
        return self._file_name("error")

    def _set_error(self, tb):
        import io
        error_output = io.StringIO()
        traceback.print_tb(tb, file=error_output)
        error_content = error_output.getvalue()

        # Use existing session if available, otherwise create a new one
        if self.storage_session:
            self.storage_session.write(self._error_file_name(), error_content)
        else:
            with self.storage_backend.session("Saving error information") as session:
                session.write(self._error_file_name(), error_content)

    def _set_last_run(self):
        timestamp = datetime.datetime.now().isoformat()

        # Use existing session if available, otherwise create a new one
        if self.storage_session:
            self.storage_session.write(self._last_run_file_name(), timestamp)
        else:
            with self.storage_backend.session("Updating last run timestamp") as session:
                session.write(self._last_run_file_name(), timestamp)

    def _get_last_run(self):
        try:
            # Use existing session if available, otherwise create a new one
            if self.storage_session:
                timestamp_str = self.storage_session.open(self._last_run_file_name())
                return parser.parse(timestamp_str)
            else:
                with self.storage_backend.session("Reading last run timestamp") as session:
                    timestamp_str = session.open(self._last_run_file_name())
                    return parser.parse(timestamp_str)
        except FileNotFoundError:
            return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        """
        This method will allow us to log uncaught exceptions.

        """
        if self.storage_session:
            if exc_type is None:
                self.finalise()
            else:
                # On error, reset session without committing
                self.storage_backend._reset_session_state(self.storage_session)

        if not exc_type:
            return

        # We don't want to log KeyboardInterrupts
        if exc_type is not KeyboardInterrupt:
            self._set_error(tb)

    def finalise(self):
        """
        Call this to wrap up any operations, e.g committing files
        """
        self._set_last_run()
        # End session if still active
        if self.storage_session:
            self.storage_backend.end_session(self.storage_session, "Scraping completed")
            self.storage_session = None

    def _save_file(self, dir_name, file_name, content):
        if not self.storage_session:
            raise RuntimeError(
                f"Cannot save file {dir_name}/{file_name}: No active storage session. "
                f"You must call start_storage_session() before saving files."
            )

        full_path = Path(dir_name)
        file_path = full_path / file_name
        self.storage_session.write(filename=file_path, content=content)

    def save_raw(self, filename, content):
        self._save_file("raw", filename, content)

    def save_json(self, obj):
        file_name = "{}.json".format(obj.as_file_name())
        self._save_file("json", file_name, obj.as_json())


    def clean_data_dir(self):
        # Note: Storage backends handle their own cleanup mechanisms
        # This method is kept for compatibility but may not be needed
        # depending on the specific storage backend implementation
        pass


class CodeCommitMixin:
    def __init__(self, options, console):
        super().__init__(options, console)

        if self.options.get("aws_lambda"):
            self.repository = self.options["council"]
            self.codecommit_client = boto3.client("codecommit")
            try:
                self.codecommit_client.get_repository(repositoryName=self.repository)
            except ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code == "RepositoryDoesNotExistException":
                    self.create_repo()
                else:
                    raise
            self.put_files = []
            self.today = datetime.datetime.now().strftime("%Y-%m-%d")
            self._branch_head = ""
            self.batch = 1
            self.log_file_path = f"{self.scraper_object_type}/logbook.json"

    @property
    def branch_head(self):
        """returns today's branch's HEAD commit hash"""
        if not self._branch_head:
            try:
                branch_info = self.codecommit_client.get_branch(
                    repositoryName=self.repository, branchName=self.branch
                )
                self._branch_head = branch_info["branch"]["commitId"]
            except self.codecommit_client.exceptions.BranchDoesNotExistException:
                self._branch_head = self.create_branch(self.branch)

        return self._branch_head

    @branch_head.setter
    def branch_head(self, commit_id):
        self._branch_head = commit_id

    @branch_head.deleter
    def branch_head(self):
        self._branch_head = ""

    @property
    def branch(self):
        """returns today's branch name"""
        return f"{self.options['council']}-{self.today}"

    def create_branch(self, branch_name):
        """
        `$ git checkout -b branch_name main`
        ...or create a branch with HEAD pointing at main

        returns commit hash of HEAD
        """
        main_info = self.codecommit_client.get_branch(
            repositoryName=self.repository, branchName="main"
        )
        commit_id = main_info["branch"]["commitId"]

        self.codecommit_client.create_branch(
            repositoryName=self.repository,
            branchName=branch_name,
            commitId=commit_id,
        )

        return commit_id

    def delete_branch(self):
        delete_info = self.codecommit_client.delete_branch(
            repositoryName=self.repository, branchName=self.branch
        )
        if delete_info["deletedBranch"]:
            self.console.log(f"deleted {delete_info['deletedBranch']['branchName']}")

    def get_files(self, folder_path):
        subfolder_paths = []
        file_paths = []
        try:
            self.console.log(f"Getting all files in {folder_path}...")
            folder = self.codecommit_client.get_folder(
                repositoryName=self.repository,
                commitSpecifier=self.branch,
                folderPath=folder_path,
            )
            for subfolder in folder["subFolders"]:
                subfolder_paths.append(subfolder["absolutePath"])
            for file in folder["files"]:
                file_paths.append(file["absolutePath"])

            for subfolder_path in subfolder_paths:
                sf_paths, f_paths = self.get_files(subfolder_path)
                subfolder_paths.extend(sf_paths)
                file_paths.extend(f_paths)

            self.console.log(f"...found {len(file_paths)} files in {folder_path}")
            return subfolder_paths, file_paths

        except self.codecommit_client.exceptions.FolderDoesNotExistException:
            self.console.log(f"{folder_path} Does not exist")
            return subfolder_paths, file_paths

    def delete_files(self, delete_files, batch):
        message = f"Deleting batch no. {batch} consisting of {len(delete_files)} files"
        self.console.log(message)

        return self.commit(message=message, delete_files=delete_files)

    def delete_existing(self, commit_id):
        _, file_paths = self.get_files(f"{self.scraper_object_type}")
        batch = 1
        while len(file_paths) >= 100:
            delete_files = [{"filePath": fp} for fp in file_paths[:100]]
            delete_commit = self.delete_files(delete_files, batch)
            batch += 1
            file_paths = file_paths[100:]
            commit_id = delete_commit["commitId"]

        if file_paths:
            delete_files = [{"filePath": fp} for fp in file_paths]
            delete_commit = self.delete_files(delete_files, batch)
            return delete_commit["commitId"]
        return commit_id

    def delete_data_if_exists(self):
        self.console.log("Deleting existing data...")
        head = self.branch_head
        delete_commit = self.delete_existing(head)
        if head == delete_commit:
            self.console.log("...no data to delete.")
        else:
            self.branch_head = delete_commit
            self.console.log("...data deleted.")

    def commit(
        self,
        message: str = "",
        put_files: list = None,
        delete_files: list = None,
    ):
        try:
            commit_info = self.codecommit_client.create_commit(
                repositoryName=self.repository,
                branchName=self.branch,
                parentCommitId=self.branch_head,
                commitMessage=message,
                putFiles=put_files if put_files else [],
                deleteFiles=delete_files if delete_files else [],
            )
        except self.codecommit_client.exceptions.ParentCommitIdOutdatedException:
            del self.branch_head
            commit_info = self.codecommit_client.create_commit(
                repositoryName=self.repository,
                branchName=self.branch,
                parentCommitId=self.branch_head,
                commitMessage=message,
                putFiles=put_files if put_files else [],
                deleteFiles=delete_files if delete_files else [],
            )
        self.branch_head = commit_info["commitId"]
        return commit_info

    def process_batch(self):
        self.console.log(
            f"Committing batch {self.batch} consisting of {len(self.put_files)} files"
        )
        message = (
            f"{self.scraper_object_type} - batch {self.batch} - scraped on {self.today}"
        )
        commit_info = self.commit(put_files=self.put_files, message=message)
        self.branch_head = commit_info["commitId"]
        self.batch += 1
        self.put_files = []

    def attempt_merge(self):
        self.console.log("Attempting to create merge commit...")
        merge_info = self.codecommit_client.merge_branches_by_squash(
            repositoryName=self.repository,
            sourceCommitSpecifier=self.branch,
            destinationCommitSpecifier="main",
            commitMessage=f"{self.scraper_object_type} - scraped on {self.today}",
        )
        self.console.log(
            f"{self.branch} squashed and merged into main at {merge_info['commitId']}"
        )

    def aws_tidy_up(self, run_log: RunLog):
        if self.options.get("aws_lambda"):
            # Check if there's anything left to commit...
            if self.put_files:
                self.process_batch()

            # check for differences
            try:
                differences_response = self.codecommit_client.get_differences(
                    repositoryName=self.repository,
                    afterCommitSpecifier=self.branch,
                    beforeCommitSpecifier="main",
                    afterPath=self.scraper_object_type,
                    beforePath=self.scraper_object_type,
                    MaxResults=400,
                )
            except self.codecommit_client.exceptions.PathDoesNotExistException:
                # The council has never been scraped before - so everything is new,
                # but we can just treat it as differences, and fake the necessary
                # bit of a differences response object
                differences_response = {"differences": True}

            if not differences_response["differences"]:
                # noinspection PyAttributeOutsideInit
                self.new_data = False
                self.console.log("No new councillor data found.")

            self.console.log(
                f"Finished attempting to scrape: {self.options['council']}"
            )

            # squash and merge
            self.commit_run_log(run_log)
            self.attempt_merge()
            self.delete_branch()

        run_log.finish()

    def get_logbook(self):
        try:
            logbook_response = self.codecommit_client.get_file(
                repositoryName=self.repository, filePath=self.log_file_path
            )
        except self.codecommit_client.exceptions.FileDoesNotExistException:
            logbook_response = self.create_log_file()

        return json.loads(logbook_response["fileContent"])

    def create_log_file(self):
        bare_log = json.dumps({"name": self.options["council"], "runs": []})
        response = self.commit(
            put_files=[
                {"filePath": self.log_file_path, "fileContent": bare_log},
            ],
            message=f"Creating empty log file for {self.options['council']}",
        )

        # construct a similar return obj to client.create_commit
        return {
            "commitId": response["commitId"],
            "blobId": response["filesAdded"][0]["blobId"],
            "filePath": response["filesAdded"][0]["absolutePath"],
            "fileContent": bare_log,
        }

    def commit_run_log(self, run_log: RunLog):
        run_log.log = (
            self.console.export_text()
        )  # maybe this wants to be export_html()?
        run_log.finish()

        log_book = self.get_logbook()
        if len(log_book["runs"]) > 20:
            log_book["runs"].pop(0)

        log_book["runs"].append(run_log.as_json)
        commit_info = self.commit(
            put_files=[
                {
                    "filePath": self.log_file_path,
                    "fileContent": json.dumps(log_book),
                }
            ],
            message=f"Logging run for {self.options['council']}",
        )
        self.console.log(f"Created log commit {commit_info['commitId']}")

    def create_repo(self):
        try:
            self.codecommit_client.create_repository(repositoryName=self.repository)
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code == "RepositoryNameExistsException":
                return
            raise
