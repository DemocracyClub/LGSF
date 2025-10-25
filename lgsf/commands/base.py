import abc
import argparse
import datetime
import traceback
from dataclasses import dataclass, field
from functools import cached_property
from typing import List

import requests
from dateutil.parser import parse
from dateutil.utils import today
from rich.console import Console
from rich.progress import BarColumn, Progress, TimeElapsedColumn
from rich.table import Table

from lgsf.conf import settings
from lgsf.path_utils import _abs_path, load_council_info, load_scraper


class CommandBase(metaclass=abc.ABCMeta):
    command_name = None

    def __init__(self, argv, stdout, pretty=False):
        self.argv = argv
        self.create_parser()
        self.stdout = stdout
        self.console = Console(file=self.stdout, record=True)
        self.pretty = pretty

    def create_parser(self):
        self.parser = argparse.ArgumentParser()
        if hasattr(self, "add_arguments"):
            self.add_arguments(self.parser)
        return self.parser.parse_args(self.argv[1:])

    def add_default_arguments(self, parser):
        """
        Entry point for subclassed commands to add custom arguments.
        """
        self.parser.add_argument(
            "-v", "--verbose", action="store_true", help="Verbose output"
        )
        self.parser.add_argument(
            "-u",
            "--unpretty",
            action="store_false",
            help="Disable pretty output (Rich)",
        )

    def execute(self):
        self.options = vars(self.create_parser())
        if "unpretty" in self.options:
            self.pretty = self.options["unpretty"]
        return self.handle(self.options)

    @abc.abstractmethod
    def handle(self, *args, **options):
        """
        The actual logic of the command. Subclasses must implement
        this method.
        """
        raise NotImplementedError(
            "subclasses of BaseCommand must provide a handle() method"
        )


@dataclass(unsafe_hash=True)
class Council:
    _metadata_cache: dict = field(
        default_factory=dict, init=False, repr=False, hash=False
    )
    council_id: str

    @property
    def metadata(self):
        if not self._metadata_cache:
            self._metadata_cache = load_council_info(self.council_id)
        return self._metadata_cache

    @property
    def current(self):
        # Check for dates in everyelectiion_data first (new format)
        everyelectiion_data = self.metadata.get("everyelectiion_data", {})
        end_date = everyelectiion_data.get("end_date") or self.metadata.get("end_date")
        start_date = everyelectiion_data.get("start_date") or self.metadata.get(
            "start_date"
        )

        if end_date and parse(end_date) < today():
            # This council has a known end date, and that date is in the past
            return False
        if start_date and parse(start_date) > today():
            # This council has a start date in the future
            return False
        return True


class CouncilFilteringCommandBase(CommandBase):
    """
    Base class for commands that need council filtering but don't run scrapers.
    Provides council selection and filtering without scraper-specific options.
    """

    def create_parser(self):
        self.parser = argparse.ArgumentParser()
        self.add_default_arguments(self.parser)
        if hasattr(self, "add_arguments"):
            self.add_arguments(self.parser)
        return self.parser.parse_args(self.argv[1:])

    @property
    def _all_council_dirs(self) -> List[str]:
        """
        Return a list of paths for each directory in the scraper directory
        """
        return [
            d.name.split("-")[0]
            for d in (settings.BASE_PATH / settings.SCRAPER_DIR_NAME).iterdir()
            if d.is_dir() and not d.name.startswith("__")
        ]

    @property
    def all_councils(self) -> List[Council]:
        return [Council(council_id) for council_id in self._all_council_dirs]

    @property
    def current_councils(self):
        return [council for council in self.all_councils if council.current]

    @cached_property
    def current_council_ids(self):
        return {c.council_id for c in self.current_councils}

    def disabled(self):
        from lgsf.path_utils import load_scraper

        disabled_councils = []
        for council in self.current_councils:
            scraper = load_scraper(council.council_id, self.command_name)
            if scraper and scraper.disabled:
                council_info = {
                    "code": council.council_id,
                    "name": council.metadata.get("official_name", "Unknown"),
                }
                disabled_councils.append(council_info)
        return sorted(disabled_councils, key=lambda d: d["code"])


class PerCouncilCommandBase(CouncilFilteringCommandBase):
    """
    For commands that operate on a list of councils and run scrapers
    """

    def create_parser(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(
            "--council",
            action="store",
            help="The 3 letter ID of the council to run this command on. "
            "Can be comma separated",
        )
        self.parser.add_argument(
            "--all-councils",
            action="store_true",
            help="Run this command for all councils",
        )
        self.parser.add_argument(
            "--exclude-missing",
            action="store_true",
            help="Don't run councils missing a scraper matching command name",
        )
        self.parser.add_argument(
            "-t",
            "--tags",
            action="store",
            help="Only run scrapers with the given tags (comma separated)",
        )
        self.parser.add_argument(
            "-r",
            "--refresh",
            action="store_true",
            help="Only run scrapers not run recently",
        )
        self.parser.add_argument(
            "--check-only",
            action="store_true",
            help="Just check for updated pages, don't scrape anything",
        )
        self.parser.add_argument(
            "--list-missing",
            action="store_true",
            help="Print missing councils",
        )
        self.parser.add_argument(
            "--list-disabled",
            action="store_true",
            help="Print disabled councils",
        )
        self.parser.add_argument(
            "--list-failing",
            action="store_true",
            help="Print failing councils",
        )

        self.add_default_arguments(self.parser)

        # Allow subclasses to add AWS support via mixin
        if hasattr(self, "add_aws_argument"):
            self.add_aws_argument(self.parser)

        if hasattr(self, "add_arguments"):
            self.add_arguments(self.parser)

        args = self.parser.parse_args(self.argv[1:])
        if args.list_missing or args.list_disabled or args.list_failing:
            return args
        if not any((args.council, args.all_councils, args.tags)):
            self.parser.error("one of --council or --all-councils or --tags required")
        if args.council and args.tags:
            self.parser.error("Can't use --tags and --council together")
        return args

    def missing(self):
        always_excluded = ["GLA", "london"]
        missing_councils = []
        for council in self.current_councils:
            if council.council_id in always_excluded:
                continue
            scraper = load_scraper(council.council_id, self.command_name)
            if not scraper:
                council_info = {
                    "code": council.council_id,
                    "name": council.metadata.get("official_name", "Unknown"),
                }
                missing_councils.append(council_info)
        return sorted(missing_councils, key=lambda d: d["code"])

    def output_missing(self):
        table = Table(title=f"Councils missing '{self.command_name}' scraper")

        table.add_column("Code", style="magenta")
        table.add_column("Name", style="green")
        for council in self.missing():
            table.add_row(council["code"], council["name"])

        self.console.print(table)

    def output_disabled(self):
        table = Table(title=f"Councils with '{self.command_name}' disabled scraper")

        table.add_column("Code", style="magenta")
        table.add_column("Name", style="green")
        for council in self.disabled():
            table.add_row(council["code"], council["name"])

        self.console.print(table)

    def failing(self):
        req = requests.get(
            "https://democracyclub.github.io/lgsf-dashboard/api/failing.json"
        )
        return req.json()

    def output_failing(self):
        table = Table(title=f"Councils with '{self.command_name}' failing")
        table.add_column("Code", style="magenta")
        table.add_column("Error", style="red")
        for council in self.failing():
            if council["council_id"] in self.current_council_ids:
                table.add_row(council["council_id"], council["latest_run"]["log_text"])
        self.console.print(table)

    def output_status(self):
        from rich.columns import Columns
        from rich.panel import Panel

        missing = str(len(self.missing()))
        disabled = str(len(self.disabled()))
        self.console.print(
            Columns(
                [
                    Panel(disabled, title="Disabled"),
                    Panel(missing, title="Missing"),
                ]
            )
        )

    @property
    def councils_to_run(self):
        councils = []
        if self.options["all_councils"] or self.options["tags"]:
            councils = self.current_councils

        else:
            for council in self.options["council"].split(","):
                council = Council(council.strip().split("-")[0].upper())
                councils.append(council)
        if self.options["exclude_missing"]:
            missing_councils = {c["code"] for c in self.missing()}
            councils = list(set(councils) - missing_councils)
        return councils

    def run_councils(self):
        for council in self.councils_to_run:
            self.run_council(council.council_id)

    def run_councils_with_progress(self):
        to_run = self.councils_to_run
        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
            console=self.console,
            auto_refresh=False,
        ) as progress:
            total = progress.add_task(description="Total", total=len(to_run))
            while not progress.finished:
                for council in to_run:
                    self.run_council(council.council_id)
                    progress.update(total, advance=1)
                    progress.refresh()

    def _run_single(self, scraper):
        run_log = settings.RUN_LOGGER(start=datetime.datetime.now(datetime.UTC))
        try:
            scraper.run(run_log)
        except KeyboardInterrupt:
            raise
        except Exception:
            run_log.error = traceback.format_exc()
            if self.options.get("verbose"):
                raise
        run_log.finish()

        self.console.print(run_log.as_rich_table)

    def run_council(self, council):
        self.options["council"] = council
        self.options["council_info"] = load_council_info(council)
        scraper_cls = load_scraper(council, self.command_name)
        if not scraper_cls:
            return
        with scraper_cls(self.options, self.console) as scraper:
            should_run = True
            if scraper.disabled:
                should_run = False
            if should_run and self.options["refresh"] and scraper.run_since():
                should_run = False
            if should_run and self.options["tags"]:
                required_tags = set(self.options["tags"].split(","))
                scraper_tags = set(scraper.get_tags)
                if not required_tags.issubset(scraper_tags):
                    should_run = False
            if should_run:
                # Clear console recording to exclude bootstrapping output from run log
                if hasattr(self.console, "_record_buffer"):
                    self.console._record_buffer.clear()
                self._run_single(scraper)

    def normalise_codes(self):
        new_codes = []
        if self.options.get("council"):
            old_codes = self.options["council"].split(",")

            for code in old_codes:
                new_codes.append(_abs_path(settings.SCRAPER_DIR_NAME, code)[1])
        self.options["council"] = ",".join(new_codes)
        return self.options

    def handle(self, options):
        self.options = options

        if options["list_missing"]:
            return self.output_missing()

        if options["list_disabled"]:
            return self.output_disabled()

        if options["list_failing"]:
            return self.output_failing()

        # Check if AWS invocation is requested via mixin
        if hasattr(self, "should_invoke_aws") and self.should_invoke_aws(options):
            return self.handle_aws_invocation(options)

        self.output_status()
        self.normalise_codes()
        if self.pretty:
            self.run_councils_with_progress()
            return None
        self.run_councils()
        return None
