"""
The Lambda worker never commits a failed run.

The handler used to call finalize_storage after the scraper raised, so a
partial scrape was committed and merged. These run the real handler with
the real local storage backend in a temporary directory; only the scraper
lookup and the CloudWatch stream are replaced.
"""

import io

import pytest

from lgsf.aws_lambda import handlers
from lgsf.conf import settings
from lgsf.councillors.scrapers import BaseCouncillorScraper
from lgsf.scrapers.base import ScraperBase
from lgsf.storage.backends.base import StorageMode
from lgsf.storage.backends.local import LocalFilesystemStorage

FULL_COUNCIL = [f"member-{i:02d}" for i in range(12)]


class ListedCouncillorScraper(BaseCouncillorScraper):
    names: list = []
    fail_at: str | None = None
    disabled = False

    def get_councillors(self):
        return [{"name": name} for name in self.names]

    def get_single_councillor(self, row):
        name = row["name"]
        if name == self.fail_at:
            raise OSError(f"site fell over at {name}")
        return self.add_councillor(
            f"https://example.gov.uk/councillors/{name}",
            identifier=name,
            name=name.title(),
            party="Independent",
            division="Ward",
        )


@pytest.fixture(autouse=True)
def lambda_environment(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    monkeypatch.delenv("LGSF_STORAGE_BACKEND", raising=False)
    monkeypatch.delenv("S3_REPORTS_BUCKET", raising=False)
    monkeypatch.setattr(handlers, "CloudWatchLogStream", lambda **kwargs: io.StringIO())

    def base_init(self, options, console):
        self.options = options
        self.console = console
        self.council_id = options["council"]
        self.base_url = "https://example.gov.uk"
        self.run_id = "run-test"
        self.execution_id = options.get("execution_id")
        self.storage_backend = LocalFilesystemStorage(
            council_code=self.council_id,
            scraper_object_type="Councillors",
            storage_mode=StorageMode.REPLACE,
        )
        self.storage_session = self.storage_backend.start_session()

    monkeypatch.setattr(ScraperBase, "__init__", base_init)
    return tmp_path


def invoke(monkeypatch, names, fail_at=None):
    cls = type(
        "Scraper", (ListedCouncillorScraper,), {"names": names, "fail_at": fail_at}
    )
    monkeypatch.setattr(handlers, "load_scraper", lambda council, command: cls)
    return handlers.scraper_worker_handler(
        {"council": "TST", "scraper_type": "councillors"}, context=None
    )


def stored(data_dir):
    """The identifiers of the councillors on disk; files are named <identifier>-<name>."""
    root = data_dir / "TST" / "Councillors" / "json"
    if not root.exists():
        return []
    return sorted(p.stem.rsplit("-", 2)[0] for p in root.iterdir())


def test_a_successful_run_is_committed_and_reported(lambda_environment, monkeypatch):
    result = invoke(monkeypatch, FULL_COUNCIL)

    assert (result["statusCode"], result["status"]) == (200, "completed")
    assert stored(lambda_environment) == FULL_COUNCIL
    # Enough for the run report to show duration and what the run produced
    assert result["end_time"] >= result["start_time"]
    assert result["duration_seconds"] >= 0
    assert result["run_id"] == "run-test"
    assert result["commit"] is None  # the local backend makes no commits


def test_a_run_that_raises_partway_commits_nothing(lambda_environment, monkeypatch):
    invoke(monkeypatch, FULL_COUNCIL)

    result = invoke(monkeypatch, FULL_COUNCIL[:6] + ["x", "y", "z"], fail_at="y")

    assert (result["statusCode"], result["status"]) == (500, "failed")
    assert result["error"].startswith("OSError: site fell over at y")
    assert stored(lambda_environment) == FULL_COUNCIL


def test_too_few_councillors_fails_the_run_and_commits_nothing(
    lambda_environment, monkeypatch
):
    invoke(monkeypatch, FULL_COUNCIL)

    result = invoke(monkeypatch, FULL_COUNCIL[:3])

    assert (result["statusCode"], result["status"]) == (500, "failed")
    assert result["error"].startswith("ValueError: Not many councillors found (3)")
    assert stored(lambda_environment) == FULL_COUNCIL
