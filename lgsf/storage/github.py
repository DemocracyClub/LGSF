import json
from typing import Any, Dict, List, Optional

from github import Github, GithubException
from .base import StorageBackend, StorageConfig

class GitHubStorage(StorageBackend):
    """Storage backend for GitHub repository operations"""

    def __init__(self, config: StorageConfig):
        super().__init__(config)
        self.token = getattr(config, 'github_token', None) or os.environ.get('GITHUB_TOKEN')
        self.repo_name = getattr(config, 'github_repo', None) or 'symroe/test-repo'
        self.branch = getattr(config, 'github_branch', None) or 'main'
        self.github = Github(self.token)
        self.repo = self.github.get_repo(self.repo_name)
        self.put_files = []
        self._branch_ref = None

    @property
    def branch_ref(self):
        if not self._branch_ref:
            self._branch_ref = self.repo.get_git_ref(f'heads/{self.branch}')
        return self._branch_ref

    def save(self, path: str, content: Any) -> None:
        if isinstance(content, (dict, list)):
            content = json.dumps(content, indent=4)
        elif not isinstance(content, str):
            content = str(content)
        self.put_files.append({
            "filePath": path,
            "fileContent": content.encode('utf-8')
        })

    def load(self, path: str) -> Any:
        try:
            file_content = self.repo.get_contents(path, ref=self.branch)
            content = file_content.decoded_content.decode('utf-8')
            if path.endswith('.json'):
                return json.loads(content)
            return content
        except GithubException as e:
            if e.status == 404:
                return None
            raise

    def delete(self, path: str) -> None:
        self.put_files.append({
            "filePath": path,
            "fileContent": None
        })

    def exists(self, path: str) -> bool:
        try:
            self.repo.get_contents(path, ref=self.branch)
            return True
        except GithubException as e:
            if e.status == 404:
                return False
            raise

    def commit(self, message: str) -> Dict:
        if not self.put_files:
            return {"commit": self.branch_ref.object.sha}
        for file in self.put_files:
            if file["fileContent"] is None:
                # Deletion
                try:
                    contents = self.repo.get_contents(file["filePath"], ref=self.branch)
                    # get_contents returns a list if path is a directory, but we only delete files
                    if isinstance(contents, list):
                        for item in contents:
                            if item.type == 'file':
                                self.repo.delete_file(item.path, message, item.sha, branch=self.branch)
                    else:
                        self.repo.delete_file(file["filePath"], message, contents.sha, branch=self.branch)
                except GithubException as e:
                    if e.status != 404:
                        raise
            else:
                try:
                    contents = self.repo.get_contents(file["filePath"], ref=self.branch)
                    self.repo.update_file(file["filePath"], message, file["fileContent"].decode('utf-8'), contents.sha, branch=self.branch)
                except GithubException as e:
                    if e.status == 404:
                        self.repo.create_file(file["filePath"], message, file["fileContent"].decode('utf-8'), branch=self.branch)
                    else:
                        raise
        self.put_files = []
        return {"commit": message}

    def get_files(self, prefix: str) -> tuple[str, List[str]]:
        contents = self.repo.get_contents(prefix, ref=self.branch)
        files = []
        for content_file in contents:
            if content_file.type == 'file':
                files.append(content_file.path)
        return self.branch_ref.object.sha, files

    def delete_existing(self, prefix: str) -> str:
        _, file_paths = self.get_files(prefix)
        for fp in file_paths:
            self.delete(fp)
        commit_info = self.commit(f"Delete all files under {prefix}")
        return commit_info["commit"] 