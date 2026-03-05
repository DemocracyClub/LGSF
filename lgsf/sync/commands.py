"""
Sync command for downloading council data from GitHub repositories.

This command syncs data from the GitHub organization's repositories
to the local data directory. It respects the per-council command pattern,
allowing syncing of individual councils, all councils, or filtered sets.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import requests
from rich.progress import BarColumn, Progress, SpinnerColumn, TimeElapsedColumn
from rich.table import Table

from lgsf.commands.base import Council, CouncilFilteringCommandBase
from lgsf.conf import settings


class GitHubSyncer:
    """
    Handles downloading files from a GitHub repository.

    Uses the GitHub API with a personal access token to download
    all files from a repository's default branch.
    """

    def __init__(
        self,
        organization: str,
        github_token: str,
        console=None,
    ):
        self.organization = organization
        self.github_token = github_token
        self.console = console
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create and configure the GitHub API session."""
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"token {self.github_token}",
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "LGSF-Sync/1.0",
            }
        )
        return session

    def repo_exists(self, repo_name: str) -> bool:
        """Check if a repository exists in the organization."""
        url = f"https://api.github.com/repos/{self.organization}/{repo_name}"
        try:
            response = self.session.get(url, timeout=30)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def get_default_branch(self, repo_name: str) -> str:
        """Get the default branch of a repository."""
        url = f"https://api.github.com/repos/{self.organization}/{repo_name}"
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json().get("default_branch", "main")
        except requests.RequestException:
            return "main"

    def get_tree(self, repo_name: str, branch: str = "main") -> list[dict]:
        """
        Get the full file tree from a repository.

        Args:
            repo_name: The repository name
            branch: The branch to get the tree from

        Returns:
            List of file objects with path, type, and sha
        """
        url = f"https://api.github.com/repos/{self.organization}/{repo_name}/git/trees/{branch}?recursive=1"
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()
            # Filter to only include blobs (files), not trees (directories)
            return [item for item in data.get("tree", []) if item["type"] == "blob"]
        except requests.RequestException as e:
            if self.console:
                self.console.print(
                    f"[red]Failed to get tree for {repo_name}: {e}[/red]"
                )
            return []

    def download_file(self, repo_name: str, file_path: str) -> Optional[bytes]:
        """
        Download a file's contents from a repository.

        Args:
            repo_name: The repository name
            file_path: Path to the file within the repository

        Returns:
            File contents as bytes, or None if download failed
        """
        url = f"https://api.github.com/repos/{self.organization}/{repo_name}/contents/{file_path}"
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()

            # GitHub returns base64-encoded content for small files
            if "content" in data:
                import base64

                return base64.b64decode(data["content"])

            # For larger files, we need to download from the raw URL
            if "download_url" in data and data["download_url"]:
                raw_response = self.session.get(data["download_url"], timeout=120)
                raw_response.raise_for_status()
                return raw_response.content

            return None
        except requests.RequestException as e:
            if self.console:
                self.console.print(
                    f"[yellow]Failed to download {file_path}: {e}[/yellow]"
                )
            return None

    def sync_repository(
        self,
        repo_name: str,
        target_dir: Path,
        show_progress: bool = True,
    ) -> dict:
        """
        Sync all files from a repository to a local directory.

        Args:
            repo_name: The repository name (usually council code)
            target_dir: Local directory to sync files to
            show_progress: Whether to show progress during sync

        Returns:
            Dict with sync statistics
        """
        stats = {
            "files_downloaded": 0,
            "files_skipped": 0,
            "errors": 0,
            "total_bytes": 0,
        }

        # Check if repo exists
        if not self.repo_exists(repo_name):
            if self.console:
                self.console.print(
                    f"[yellow]Repository {self.organization}/{repo_name} not found[/yellow]"
                )
            return {"skipped": True, "reason": "repository_not_found"}

        # Get the default branch
        branch = self.get_default_branch(repo_name)

        # Get file tree
        files = self.get_tree(repo_name, branch)
        if not files:
            return {"skipped": True, "reason": "no_files"}

        # Filter out README and other non-data files
        data_files = [
            f
            for f in files
            if not f["path"].startswith(".")
            and f["path"] != "README.md"
            and f["path"] != "LICENSE"
        ]

        if not data_files:
            return {"skipped": True, "reason": "no_data_files"}

        # Clean target directory
        if target_dir.exists():
            shutil.rmtree(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        # Download files
        if show_progress and self.console:
            with Progress(
                SpinnerColumn(),
                "[progress.description]{task.description}",
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn(),
                console=self.console,
                transient=True,
            ) as progress:
                task = progress.add_task(
                    f"Downloading {repo_name}", total=len(data_files)
                )
                for file_info in data_files:
                    file_path = file_info["path"]
                    content = self.download_file(repo_name, file_path)
                    if content is not None:
                        local_path = target_dir / file_path
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        local_path.write_bytes(content)
                        stats["files_downloaded"] += 1
                        stats["total_bytes"] += len(content)
                    else:
                        stats["errors"] += 1
                    progress.update(task, advance=1)
        else:
            for file_info in data_files:
                file_path = file_info["path"]
                content = self.download_file(repo_name, file_path)
                if content is not None:
                    local_path = target_dir / file_path
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    local_path.write_bytes(content)
                    stats["files_downloaded"] += 1
                    stats["total_bytes"] += len(content)
                else:
                    stats["errors"] += 1

        return stats

    def sync_repository_git(
        self,
        repo_name: str,
        target_dir: Path,
        show_progress: bool = True,
    ) -> dict:
        """
        Sync a repository using shallow git clone.

        This is faster than the API method and avoids rate limits.
        Only fetches the latest state of the default branch (no history).

        Args:
            repo_name: The repository name (usually council code)
            target_dir: Local directory to sync files to
            show_progress: Whether to show progress during sync

        Returns:
            Dict with sync statistics
        """
        repo_url = f"https://x-access-token:{self.github_token}@github.com/{self.organization}/{repo_name}.git"

        # Clean target directory
        if target_dir.exists():
            shutil.rmtree(target_dir)

        # Create parent directory
        target_dir.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Shallow clone with depth=1 (only latest commit, no history)
            cmd = [
                "git",
                "clone",
                "--depth",
                "1",
                "--single-branch",
                repo_url,
                str(target_dir),
            ]

            if show_progress and self.console:
                self.console.print(f"  [dim]Cloning {repo_name}...[/dim]")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode != 0:
                # Check if repo doesn't exist
                if "not found" in result.stderr.lower() or "404" in result.stderr:
                    return {"skipped": True, "reason": "repository_not_found"}
                if self.console:
                    self.console.print(f"[red]Git clone failed: {result.stderr}[/red]")
                return {"skipped": True, "reason": f"git_error: {result.stderr[:100]}"}

            # Remove .git directory to save space and avoid confusion
            git_dir = target_dir / ".git"
            if git_dir.exists():
                shutil.rmtree(git_dir)

            # Remove README.md and other non-data files
            for filename in ["README.md", "LICENSE", ".gitignore"]:
                filepath = target_dir / filename
                if filepath.exists():
                    filepath.unlink()

            # Count files and calculate size
            files_count = 0
            total_bytes = 0
            for file_path in target_dir.rglob("*"):
                if file_path.is_file():
                    files_count += 1
                    total_bytes += file_path.stat().st_size

            return {
                "files_downloaded": files_count,
                "total_bytes": total_bytes,
                "errors": 0,
                "method": "git",
            }

        except subprocess.TimeoutExpired:
            return {"skipped": True, "reason": "git_timeout"}
        except FileNotFoundError:
            return {"skipped": True, "reason": "git_not_installed"}
        except Exception as e:
            return {"skipped": True, "reason": f"git_error: {str(e)[:100]}"}


class Command(CouncilFilteringCommandBase):
    """
    Sync command for downloading council data from GitHub.

    This command downloads data from GitHub repositories to the local
    data directory. Each council has its own repository in the configured
    GitHub organization.

    Usage:
        python manage.py sync --council ABC
        python manage.py sync --council ABC,XYZ
        python manage.py sync --all-councils
    """

    command_name = "sync"

    def create_parser(self):
        """Override to add our own arguments and validation."""
        import argparse

        self.parser = argparse.ArgumentParser(
            description="Sync council data from GitHub repositories"
        )
        self.parser.add_argument(
            "--council",
            action="store",
            help="The council code(s) to sync (comma-separated)",
        )
        self.parser.add_argument(
            "--all-councils",
            action="store_true",
            help="Sync all current councils",
        )
        self.parser.add_argument(
            "--current-only",
            action="store_true",
            help="Only sync current councils (filters out ended/future councils)",
        )
        self.parser.add_argument(
            "--include-non-current",
            action="store_true",
            help="Include non-current councils when using --all-councils",
        )
        self.parser.add_argument(
            "--organization",
            action="store",
            default=os.environ.get("GITHUB_ORGANIZATION", "LGSF-Data"),
            help="GitHub organization name",
        )
        self.parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be synced without downloading",
        )
        self.parser.add_argument(
            "--use-git",
            action="store_true",
            help="Use shallow git clone instead of API (faster, avoids rate limits)",
        )
        self.add_default_arguments(self.parser)

        args = self.parser.parse_args(self.argv[1:])

        if not any((args.council, args.all_councils)):
            self.parser.error("one of --council or --all-councils required")

        return args

    def _load_github_token(self) -> Optional[str]:
        """
        Load GitHub token from .env file or environment.

        The .env file can contain either:
        - Just the token on a single line
        - KEY=VALUE format (e.g., GITHUB_TOKEN=ghp_xxx)

        Returns:
            The GitHub token or None if not found
        """
        # Try environment variable first
        token = os.environ.get("GITHUB_TOKEN")
        if token:
            return token

        # Try GITHUB_API_TOKEN as fallback
        token = os.environ.get("GITHUB_API_TOKEN")
        if token:
            return token

        # Try loading from .env file
        env_path = settings.BASE_PATH / ".env"
        if env_path.exists():
            with open(env_path) as f:
                content = f.read().strip()

                # If it's a simple token (no = sign), use it directly
                if "=" not in content and content.startswith("ghp_"):
                    return content

                # Parse KEY=VALUE format
                for line in content.splitlines():
                    line = line.strip()
                    if line.startswith("#") or not line:
                        continue
                    if "=" in line:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip().strip("'\"")
                        if key in ("GITHUB_TOKEN", "GITHUB_API_TOKEN"):
                            return value

        return None

    @property
    def councils_to_sync(self) -> list[Council]:
        """Get the list of councils to sync based on options."""
        if self.options.get("all_councils"):
            if self.options.get("include_non_current"):
                return self.all_councils
            return self.current_councils

        councils = []
        if self.options.get("council"):
            for code in self.options["council"].split(","):
                code = code.strip().upper()
                councils.append(Council(code))

        # Filter to current only if requested
        if self.options.get("current_only"):
            councils = [c for c in councils if c.current]

        return councils

    def sync_council(self, council: Council, syncer: GitHubSyncer) -> dict:
        """
        Sync a single council's data from GitHub.

        Args:
            council: The council to sync
            syncer: The GitHubSyncer instance

        Returns:
            Dict with sync results
        """
        repo_name = council.council_id.upper()
        target_dir = Path(settings.DATA_DIR_NAME) / council.council_id.upper()

        if self.options.get("dry_run"):
            exists = syncer.repo_exists(repo_name)
            return {
                "council": council.council_id,
                "dry_run": True,
                "repo_exists": exists,
                "target_dir": str(target_dir),
            }

        # Use git clone method if requested
        if self.options.get("use_git"):
            result = syncer.sync_repository_git(
                repo_name=repo_name,
                target_dir=target_dir,
                show_progress=self.pretty,
            )
        else:
            result = syncer.sync_repository(
                repo_name=repo_name,
                target_dir=target_dir,
                show_progress=self.pretty,
            )
        result["council"] = council.council_id
        return result

    def handle(self, options):
        """Handle the sync command."""
        self.options = options

        # Load GitHub token
        github_token = self._load_github_token()
        if not github_token:
            self.console.print(
                "[red]Error: GitHub token not found.[/red]\n"
                "Please set GITHUB_TOKEN environment variable or add it to .env file."
            )
            return 1

        organization = options.get("organization", "dc-councillors")

        # Create syncer
        syncer = GitHubSyncer(
            organization=organization,
            github_token=github_token,
            console=self.console,
        )

        councils = self.councils_to_sync
        if not councils:
            self.console.print("[yellow]No councils to sync[/yellow]")
            return 0

        self.console.print(
            f"[bold]Syncing {len(councils)} council(s) from {organization}[/bold]\n"
        )

        results = []
        for council in councils:
            self.console.print(f"[cyan]Syncing {council.council_id}...[/cyan]")
            result = self.sync_council(council, syncer)
            results.append(result)

            # Print result
            if result.get("dry_run"):
                status = "exists" if result.get("repo_exists") else "not found"
                self.console.print(f"  Repository: {status}")
            elif result.get("skipped"):
                self.console.print(
                    f"  [yellow]Skipped: {result.get('reason')}[/yellow]"
                )
            else:
                files = result.get("files_downloaded", 0)
                errors = result.get("errors", 0)
                bytes_dl = result.get("total_bytes", 0)
                size_str = self._format_bytes(bytes_dl)
                if errors:
                    self.console.print(
                        f"  [green]Downloaded {files} files ({size_str})[/green], "
                        f"[red]{errors} errors[/red]"
                    )
                else:
                    self.console.print(
                        f"  [green]Downloaded {files} files ({size_str})[/green]"
                    )

        # Summary table
        self._print_summary(results)
        return 0

    def _format_bytes(self, num_bytes: int) -> str:
        """Format bytes as human-readable string."""
        for unit in ["B", "KB", "MB", "GB"]:
            if num_bytes < 1024:
                return f"{num_bytes:.1f} {unit}"
            num_bytes /= 1024
        return f"{num_bytes:.1f} TB"

    def _print_summary(self, results: list[dict]):
        """Print a summary table of sync results."""
        table = Table(title="\nSync Summary")
        table.add_column("Council", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Files")
        table.add_column("Size")

        total_files = 0
        total_bytes = 0
        success_count = 0

        for result in results:
            council = result.get("council", "?")

            if result.get("dry_run"):
                status = "exists" if result.get("repo_exists") else "not found"
                table.add_row(council, f"[yellow]dry-run: {status}[/yellow]", "-", "-")
            elif result.get("skipped"):
                table.add_row(
                    council, f"[yellow]{result.get('reason')}[/yellow]", "-", "-"
                )
            else:
                files = result.get("files_downloaded", 0)
                errors = result.get("errors", 0)
                bytes_dl = result.get("total_bytes", 0)

                total_files += files
                total_bytes += bytes_dl

                if errors:
                    status = f"[red]{errors} errors[/red]"
                else:
                    status = "[green]OK[/green]"
                    success_count += 1

                table.add_row(council, status, str(files), self._format_bytes(bytes_dl))

        self.console.print(table)
        self.console.print(
            f"\n[bold]Total: {total_files} files, {self._format_bytes(total_bytes)}[/bold]"
        )
