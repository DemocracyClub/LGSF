import abc
import os
from dateutil import parser
import datetime
import traceback

import requests

# import requests_cache

# requests_cache.install_cache("scraper_cache", expire_after=60 * 60 * 24)


from lgsf.path_utils import data_abs_path
from .checks import ScraperChecker


class ScraperBase(metaclass=abc.ABCMeta):
    """
    Base class for a scraper. All scrapers should inherit from this.
    """

    disabled = False

    def __init__(self, options, console):
        self.options = options
        self.console = console
        self.check()

    def get(self, url, verify=True):
        """
        Wraps `requests.get`
        """

        if self.options.get("verbose"):
            self.console.log(f"Scraping from {url}")
        headers = {"User-Agent": "Scraper/DemocracyClub", "Accept": "*/*"}

        return requests.get(url, headers=headers, verify=verify)

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
