import abc
import json
import os

import boto3
from botocore.exceptions import ClientError
from dateutil import parser
import datetime
import traceback

import requests

# import requests_cache

# requests_cache.install_cache("scraper_cache", expire_after=60 * 60 * 24)
from requests import Session

from lgsf.path_utils import data_abs_path
from .checks import ScraperChecker


class ScraperBase(metaclass=abc.ABCMeta):
    """
    Base class for a scraper. All scrapers should inherit from this.
    """

    disabled = False
    extra_headers = {}

    def __init__(self, options, console):
        self.options = options
        self.console = console
        self.check()
        self.requests_session = Session()

    def get(self, url, verify=True, extra_headers=None):
        """
        Wraps `requests.get`
        """

        if self.options.get("verbose"):
            self.console.log(f"Scraping from {url}")
        headers = {"User-Agent": "Scraper/DemocracyClub", "Accept": "*/*"}

        if extra_headers:
            headers.update(extra_headers)

        response = self.requests_session.get(url, headers=headers, verify=verify)
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

    def _file_name(self, name):
        dir_name = data_abs_path(self.options["council"])
        os.makedirs(dir_name, exist_ok=True)
        return os.path.join(dir_name, name)

    def _last_run_file_name(self):
        return self._file_name("_last-run")

    def _error_file_name(self):
        return self._file_name("error")

    def _set_error(self, tb):
        with open(self._error_file_name(), "w") as f:
            traceback.print_tb(tb, file=f)

    def _set_last_run(self):
        file_name = self._last_run_file_name()
        with open(file_name, "w") as f:
            f.write(datetime.datetime.now().isoformat())

    def _get_last_run(self):
        file_name = self._last_run_file_name()
        if os.path.exists(self._last_run_file_name()):
            return parser.parse(open(file_name, "r").read())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if not exc_type:
            self._set_last_run()
        else:
            # We don't want to log KeyboardInterrupts
            if not exc_type == KeyboardInterrupt:
                self._set_error(tb)

    def _save_file(self, dir_name, file_name, content):
        dir_name = data_abs_path(self.options["council"], dir_name)
        os.makedirs(dir_name, exist_ok=True)
        file_name = os.path.join(dir_name, file_name)
        with open(file_name, "w") as f:
            f.write(content)

    def save_raw(self, filename, content):
        self._save_file("raw", filename, content)

    def save_json(self, obj):
        file_name = "{}.json".format(obj.as_file_name())
        self._save_file("json", file_name, obj.as_json())


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
            self.log_file_path = f"{self.options['council']}/logbook.json"

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
            repositoryName=self.repository, branchName=branch_name, commitId=commit_id
        )

        return commit_id

    def delete_branch(self):
        delete_info = self.codecommit_client.delete_branch(
            repositoryName=self.repository, branchName=self.branch
        )
        if delete_info["deletedBranch"]:
            self.console.log(f'deleted {delete_info["deletedBranch"]["branchName"]}')

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

        delete_commit = self.commit(message=message, delete_files=delete_files)
        return delete_commit

    def delete_existing(self, commit_id):
        _, file_paths = self.get_files(f"{self.options['council']}")
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
        else:
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
        self, message: str = "", put_files: list = None, delete_files: list = None
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
            f"{self.options['council']} - batch {self.batch} - scraped on {self.today}"
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
            commitMessage=f"{self.options['council']} - scraped on {self.today}",
        )
        self.console.log(
            f"{self.branch} squashed and merged into main at {merge_info['commitId']}"
        )

    def aws_tidy_up(self, run_log: "lgsf.aws_lambda.run_log.RunLog"):
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
                    afterPath=self.options["council"],
                    beforePath=self.options["council"],
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

    def commit_run_log(self, run_log: "lgsf.aws_lambda.RunLog"):
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
            else:
                raise
