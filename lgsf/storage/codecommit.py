import json
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from .base import CodeCommitStorage, StorageConfig

class CodeCommitStorage(CodeCommitStorage):
    """Storage backend for AWS CodeCommit operations"""

    def __init__(self, config: StorageConfig):
        super().__init__(config)
        self.codecommit_client = boto3.client('codecommit')
        self.repository = "CouncillorsRepo"
        self.branch = "master"
        self.branch_head = self._get_branch_head()
        self.put_files = []

    def _get_branch_head(self) -> str:
        """Get the current head commit ID of the branch"""
        response = self.codecommit_client.get_branch(
            repositoryName=self.repository,
            branchName=self.branch
        )
        return response['branch']['commitId']

    def save(self, path: str, content: Any) -> None:
        """Stage content to be saved in CodeCommit"""
        if isinstance(content, (dict, list)):
            content = json.dumps(content, indent=4)
        elif not isinstance(content, str):
            content = str(content)

        self.put_files.append({
            "filePath": path,
            "fileContent": content.encode('utf-8')
        })

    def load(self, path: str) -> Any:
        """Load content from CodeCommit"""
        try:
            response = self.codecommit_client.get_file(
                repositoryName=self.repository,
                commitSpecifier=self.branch_head,
                filePath=path
            )
            content = response['fileContent'].decode('utf-8')
            if path.endswith('.json'):
                return json.loads(content)
            return content
        except ClientError as e:
            if e.response['Error']['Code'] == 'FileDoesNotExistException':
                return None
            raise

    def delete(self, path: str) -> None:
        """Delete content from CodeCommit"""
        self.put_files.append({
            "filePath": path,
            "fileContent": None
        })

    def exists(self, path: str) -> bool:
        """Check if content exists in CodeCommit"""
        try:
            self.codecommit_client.get_file(
                repositoryName=self.repository,
                commitSpecifier=self.branch_head,
                filePath=path
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'FileDoesNotExistException':
                return False
            raise

    def commit(self, message: str) -> Dict:
        """Commit staged changes to CodeCommit"""
        if not self.put_files:
            return {"commitId": self.branch_head}

        response = self.codecommit_client.create_commit(
            repositoryName=self.repository,
            branchName=self.branch,
            parentCommitId=self.branch_head,
            commitMessage=message,
            putFiles=self.put_files
        )
        self.branch_head = response['commitId']
        self.put_files = []
        return response

    def get_files(self, prefix: str) -> tuple[str, List[str]]:
        """Get all files with the given prefix"""
        response = self.codecommit_client.get_folder(
            repositoryName=self.repository,
            commitSpecifier=self.branch_head,
            folderPath=prefix
        )
        return self.branch_head, [f['absolutePath'] for f in response['files']]

    def delete_existing(self, prefix: str) -> str:
        """Delete all files with the given prefix"""
        _, file_paths = self.get_files(prefix)
        batch = 1
        commit_id = self.branch_head

        while len(file_paths) >= 100:
            for fp in file_paths[:100]:
                self.delete(fp)
            response = self.commit(f"Batch delete {batch}")
            batch += 1
            file_paths = file_paths[100:]
            commit_id = response['commitId']

        if file_paths:
            for fp in file_paths:
                self.delete(fp)
            response = self.commit(f"Final batch delete {batch}")
            commit_id = response['commitId']

        return commit_id
