"""
A set of storage classes for scrapers to use

"""
import abc
import datetime
import json
import os
from abc import ABC
from dataclasses import dataclass, field
from typing import List
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_codecommit import CodeCommitClient

from lgsf.conf import settings
from lgsf.path_utils import data_abs_path
from lgsf.scrapers import ScraperBase


class BaseStorage(ABC):
    scraper: ScraperBase = None
    today: datetime.datetime = datetime.datetime.now().strftime("%Y-%m-%d")

    @abc.abstractmethod
    def pre_run(self):
        pass

    @abc.abstractmethod
    def save_file(self, model, content_string):
        pass

    @abc.abstractmethod
    def post_run(self):
        pass


@dataclass
class LocalFileStorage(BaseStorage):
    base_path: str = settings.DATA_DIR_NAME

    def pre_run(self):
        pass

    def save_file(self, model, content_string):
        file_name = "{}.{}".format(model.as_file_name(), self.scraper.ext)
        self.save_raw(file_name, content_string)
        self.save_json(model)

    def post_run(self):
        pass

    def _save_file(self, dir_name, file_name, content):
        dir_name = data_abs_path(self.scraper.options["council"], dir_name)
        os.makedirs(dir_name, exist_ok=True)
        file_name = os.path.join(dir_name, file_name)
        with open(file_name, "w") as f:
            f.write(content)

    def save_raw(self, filename, content):
        self._save_file("raw", filename, content)

    def save_json(self, obj):
        file_name = "{}.json".format(obj.as_file_name())
        self._save_file("json", file_name, obj.as_json())


@dataclass
class CodeCommitStorage(BaseStorage):
    repository: str

    put_files: List = field(default_factory=list)
    _branch_head: str = ""
    batch: int = 1

    def __post_init__(self):
        self.codecommit_client: CodeCommitClient = boto3.client("codecommit")
        try:
            self.codecommit_client.get_repository(repositoryName=self.repository)
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code == "RepositoryDoesNotExistException":
                self.create_repo()
            else:
                raise

    def pre_run(self):
        self.delete_data_if_exists()

    def save_file(self, model, content_string):
        council = self.scraper.options["council"]
        json_file_path = f"{self.scraper.scraper_object_type}/json/{model.as_file_name()}.json"
        raw_file_path = f"{self.scraper.scraper_object_type}/raw/{model.as_file_name()}.html"
        self.put_files.extend(
            [
                {
                    "filePath": json_file_path,
                    "fileContent": bytes(
                        json.dumps(model.as_dict(), indent=4), "utf-8"
                    ),
                },
                {
                    "filePath": raw_file_path,
                    "fileContent": bytes(content_string, "utf-8"),
                },
            ]
        )

    def create_repo(self):
        try:
            self.codecommit_client.create_repository(repositoryName=self.repository)
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code == "RepositoryNameExistsException":
                return
            else:
                raise

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
        return f"{self.today}"

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
            self.scraper.console.log(f'deleted {delete_info["deletedBranch"]["branchName"]}')

    def get_files(self, folder_path):
        subfolder_paths = []
        file_paths = []
        try:
            self.scraper.console.log(f"Getting all files in {folder_path}...")
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

            self.scraper.console.log(f"...found {len(file_paths)} files in {folder_path}")
            return subfolder_paths, file_paths

        except self.codecommit_client.exceptions.FolderDoesNotExistException:
            self.scraper.console.log(f"{folder_path} Does not exist")
            return subfolder_paths, file_paths

    def delete_files(self, delete_files, batch):
        message = f"Deleting batch no. {batch} consisting of {len(delete_files)} files"
        self.scraper.console.log(message)

        delete_commit = self.commit(message=message, delete_files=delete_files)
        return delete_commit

    def delete_existing(self, commit_id):
        _, file_paths = self.get_files(f"{self.scraper.scraper_object_type}")
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
        self.scraper.console.log("Deleting existing data...")
        head = self.branch_head
        delete_commit = self.delete_existing(head)
        if head == delete_commit:
            self.scraper.console.log("...no data to delete.")
        else:
            self.branch_head = delete_commit
            self.scraper.console.log("...data deleted.")

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

    def process_batch(self, batch):
        self.scraper.console.log(
            f"Committing batch {self.batch} consisting of {len(batch)} files"
        )
        message = (
            f"{self.scraper.scraper_object_type} - batch {self.batch} - scraped on {self.today}"
        )
        commit_info = self.commit(put_files=batch, message=message)
        self.branch_head = commit_info["commitId"]
        self.batch += 1

    def attempt_merge(self):
        self.scraper.console.log("Attempting to create merge commit...")
        merge_info = self.codecommit_client.merge_branches_by_squash(
            repositoryName=self.repository,
            sourceCommitSpecifier=self.branch,
            destinationCommitSpecifier="main",
            commitMessage=f"{self.scraper.scraper_object_type} - scraped on {self.today}",
        )
        self.scraper.console.log(
            f"{self.branch} squashed and merged into main at {merge_info['commitId']}"
        )

    def post_run(self):
        batch_size = 90
        # Check if there's anything left to commit...
        if self.put_files:
            for i in range(0, len(self.put_files), batch_size):
                self.process_batch(self.put_files[i:i+batch_size])
        self.put_files = []
        # check for differences
        try:
            differences_response = self.codecommit_client.get_differences(
                repositoryName=self.repository,
                afterCommitSpecifier=self.branch,
                beforeCommitSpecifier="main",
                afterPath=self.scraper.scraper_object_type,
                beforePath=self.scraper.scraper_object_type,
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
            self.scraper.console.log("No new councillor data found.")

        self.scraper.console.log(f"Finished attempting to scrape: {self.scraper.options['council']}")

        # squash and merge
        self.attempt_merge()
        self.delete_branch()
