"""
The GitHub backend's git path, against repositories on this machine.

One bare repository per council under a directory, as the LGSF-Data
organisation has one repository per council, and the same session code
as production: the same ``<Type>/`` layout, the same single commit per
run with the same message and trailers, the same "nothing changed, no
commit", the same "the run failed, no commit", the same fast-forward
push. The only differences are where the remote is and that a missing
repository is made with ``git init --bare`` rather than the GitHub API.

So the whole journey can be watched on a laptop: a real scraper commits
to a repository here, and a reader such as reps-wiki imports from it.

    LGSF_STORAGE_BACKEND=git LGSF_GIT_REMOTE=/tmp/lgsf-repos \\
        python manage.py councillors --council KIR
    git -C /tmp/lgsf-repos/KIR.git log --format='%h %s'

``LGSF_GIT_WORKDIR`` says where the clones are made while a run is
writing (default: ``lgsf-git-work`` under the system temp directory).
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, override

from lgsf.storage.backends.base import BaseStorage, StorageMode, StorageSession
from lgsf.storage.backends.github import GitHubStorage, _GitHubSession


class _LocalGitSession(_GitHubSession):
    """A GitHub session whose remote is a bare repository on disk and which never calls the API."""

    def __init__(
        self,
        remote_root: Path,
        workdir: Path,
        council_code: str,
        scraper_object_type: str,
        run_id: str | None,
        storage_mode: StorageMode,
    ):
        super().__init__(
            organization="local",
            github_token="",
            council_code=council_code,
            scraper_object_type=scraper_object_type,
            run_id=run_id,
            storage_mode=storage_mode,
        )
        self.bare_repo = Path(remote_root) / f"{self.repo}.git"
        self.repository_url = str(self.bare_repo)
        self.remote_url = str(self.bare_repo)
        self.local_repo_path = str(Path(workdir) / self.council_code)

    @override
    def _repo_exists_on_github(self) -> bool:
        return (self.bare_repo / "HEAD").exists()

    @override
    def _create_repository(self) -> None:
        """What creating the repository on GitHub does: an empty repository, which _initialize_empty_repo then gives its first commit."""
        self.bare_repo.parent.mkdir(parents=True, exist_ok=True)
        self._run_git_command(
            [
                "init",
                "--bare",
                "--quiet",
                f"--initial-branch={self._default_branch}",
                str(self.bare_repo),
            ],
            cwd=str(self.bare_repo.parent),
        )

    @override
    def _check_if_repo_is_empty(self) -> bool:
        heads = self._run_git_command(
            ["ls-remote", "--heads", str(self.bare_repo), self._default_branch],
            cwd=str(self.bare_repo.parent),
        )
        return not heads.stdout.strip()


class LocalGitStorage(GitHubStorage):
    """
    Commits to a bare git repository per council on this machine, through
    the GitHub backend's own session code. See the module docstring.
    """

    def __init__(
        self,
        council_code: str,
        scraper_object_type: str = "Data",
        storage_mode: StorageMode = StorageMode.REPLACE,
        remote_root: str | os.PathLike | None = None,
        workdir: str | os.PathLike | None = None,
    ):
        # Not GitHubStorage.__init__: that needs an organisation and App credentials.
        BaseStorage.__init__(self, council_code, storage_mode=storage_mode)
        self.scraper_object_type = scraper_object_type
        remote_root = remote_root or os.environ.get("LGSF_GIT_REMOTE")
        if not remote_root:
            raise ValueError(
                "The git backend needs a directory for its repositories: set "
                "LGSF_GIT_REMOTE or pass remote_root."
            )
        self.remote_root = Path(remote_root)
        self.workdir = Path(
            workdir
            or os.environ.get("LGSF_GIT_WORKDIR")
            or Path(tempfile.gettempdir()) / "lgsf-git-work"
        )
        self._active: _GitHubSession | None = None

    @override
    def _start_session(self, **kwargs: Any) -> StorageSession:
        if self._active is not None:
            raise RuntimeError(
                "A session is already active on this LocalGitStorage instance."
            )
        session = _LocalGitSession(
            remote_root=self.remote_root,
            workdir=self.workdir,
            council_code=self.council_code,
            scraper_object_type=kwargs.get(
                "scraper_object_type", self.scraper_object_type
            ),
            run_id=kwargs.get("run_id"),
            storage_mode=self.storage_mode,
        )
        self._active = session
        return session
