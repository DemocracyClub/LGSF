import abc
import datetime
import traceback
from pathlib import Path

import httpx
import requests
from dateutil import parser

from ..metadata.models import CouncilMetadata
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
    service_name = None
    scraper_object_type = None

    def __init__(self, options, console):
        self.options = options
        self.console = console
        self.check()

        self.council_id = self.options["council"]

        self.council_metadata = CouncilMetadata.for_council(self.council_id)
        self.base_url = self.council_metadata.get_service_metadata(
            self.service_name
        ).base_url

        self.storage_backend = get_storage_backend(
            council_code=self.council_id,
            options=self.options,
            scraper_object_type=self.scraper_object_type,
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
        assert self.service_name, "Scrapers must set a service_name"
        assert self.scraper_object_type, "Scrapers must set a scraper object type"
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
                with self.storage_backend.session(
                    "Reading last run timestamp"
                ) as session:
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
            self.storage_backend.end_session(
                self.storage_session,
                f"Scraping {self.service_name} for {self.options['council']} completed",
            )
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

    def prepare_storage(self, run_log=None):
        """
        Prepare the storage backend by starting a new session.
        Storage backends handle their own preparation during session creation.

        Args:
            run_log: Optional run log for recording operations
        """
        # Storage preparation is handled automatically in start_session()
        # Each backend can do its own cleanup/setup during session creation
        pass

    def finalize_storage(self, run_log=None):
        """
        Finalize the storage backend by ending the session with run log data.

        Args:
            run_log: Optional run log for recording operations
        """
        if self.storage_session:
            # Ensure the run log has console output if supported
            if (
                run_log
                and hasattr(run_log, "log")
                and hasattr(self.console, "export_text")
            ):
                if not getattr(run_log, "log", None):
                    run_log.log = self.console.export_text()

            # End session with run log for backend-specific finalization
            commit_message = f"Updated {self.options['council']}"
            self.storage_backend.end_session(
                self.storage_session, commit_message, run_log=run_log
            )
            self.storage_session = None
