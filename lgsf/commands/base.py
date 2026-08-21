import abc
import argparse
import datetime
import io
import json
import os
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dataclasses import dataclass, field
from functools import cached_property
from typing import List, Optional

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
    def name(self):
        """
        The council's name for display.

        metadata.json keeps the name under everyelectiion_data. Some files
        are in an older flat format with it at the top level, so both are
        checked, falling back to the council code.
        """
        everyelection = self.metadata.get("everyelectiion_data", {})
        return (
            everyelection.get("official_name")
            or everyelection.get("common_name")
            # Legacy flat format
            or self.metadata.get("official_name")
            or self.council_id
        )

    def service_metadata(self, service_name):
        """
        This council's recorded config for one service, or an empty dict.

        Deliberately takes the service name rather than defaulting to
        councillors: a listing for one data type showing another type's
        config reads as though the data is there when it isn't.
        """
        services = self.metadata.get("services") or {}
        return services.get(service_name) or {}

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
                disabled_councils.append(
                    {"code": council.council_id, "name": council.name}
                )
        return sorted(disabled_councils, key=lambda d: d["code"])


class PerCouncilCommandBase(CouncilFilteringCommandBase):
    """
    For commands that operate on a list of councils and run scrapers
    """

    def __init__(self, argv, stdout, pretty=False):
        super().__init__(argv, stdout, pretty)
        self.scraped_items = []  # Store scraped objects for reporting
        self._results = []
        self._lock = threading.Lock()
        self._concurrent = False
        self._council_count = 0

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
        self.parser.add_argument(
            "--workers",
            type=int,
            default=4,
            help="How many scrapers to run at once. Use 1 to watch a "
            "single scraper's output.",
        )
        self.parser.add_argument(
            "--report",
            action="store_true",
            help="Display a table report of scraped data after running",
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
        missing_councils = []
        for council in self.current_councils:
            scraper = load_scraper(council.council_id, self.command_name)
            if not scraper:
                missing_councils.append(
                    {"code": council.council_id, "name": council.name}
                )
        return sorted(missing_councils, key=lambda d: d["code"])

    def output_missing(self):
        missing = self.missing()
        table = Table(
            title=f"{len(missing)} councils missing a '{self.command_name}' scraper"
        )

        table.add_column("Code", style="magenta")
        table.add_column("Name", style="green")
        for council in missing:
            table.add_row(council["code"], council["name"])

        self.console.print(table)

    def output_disabled(self):
        table = Table(title=f"Councils with '{self.command_name}' disabled scraper")

        table.add_column("Code", style="magenta")
        table.add_column("Name", style="green")
        for council in self.disabled():
            table.add_row(council["code"], council["name"])

        self.console.print(table)

    #: URL listing the scrapers failing in production, for command types
    #: that have one. The dashboard reports a single scheduled job with no
    #: service field, so it describes exactly one data type and cannot be
    #: reused for the others.
    failing_api_url = None

    def local_run_logs(self):
        """
        The last recorded run for each council of this scraper type.

        Only councils that have been run on this machine appear, so an
        empty result means "nothing has been run here", not "nothing is
        failing".
        """
        logs = {}
        for council in self.current_councils:
            scraper_cls = load_scraper(council.council_id, self.command_name)
            if not scraper_cls or not scraper_cls.scraper_object_type:
                continue
            path = (
                Path(settings.DATA_DIR_NAME)
                / council.council_id
                / scraper_cls.scraper_object_type
                / self.RUN_LOG_FILE_NAME
            )
            if not path.is_file():
                continue
            try:
                logs[council.council_id] = json.loads(path.read_text())
            except ValueError:
                continue
        return logs

    def failing(self):
        if not self.failing_api_url:
            return []
        req = requests.get(self.failing_api_url)
        return req.json()

    def output_failing(self):
        logs = self.local_run_logs()
        if logs:
            failed = {code: log for code, log in logs.items() if log.get("status_code")}
            table = Table(
                title=f"{self.command_name}: {len(failed)} of {len(logs)} councils "
                f"failing in the last local run"
            )
            table.add_column("Code", style="magenta")
            table.add_column("When", style="cyan")
            table.add_column("Error", style="red", overflow="fold")
            for code, log in sorted(failed.items()):
                error = (log.get("error") or "").strip().splitlines()
                table.add_row(
                    code,
                    str(log.get("start", ""))[:19],
                    error[-1][:160] if error else "",
                )
            self.console.print(table)
            self.console.print(
                f"[dim]From {len(logs)} councils run locally. Councils never run "
                f"here do not appear.[/dim]"
            )
            return

        if self.failing_api_url:
            table = Table(
                title=f"Councils with '{self.command_name}' failing in production"
            )
            table.add_column("Code", style="magenta")
            table.add_column("Error", style="red")
            for council in self.failing():
                if council["council_id"] in self.current_council_ids:
                    table.add_row(
                        council["council_id"], council["latest_run"]["log_text"]
                    )
            self.console.print(table)
            return

        self.console.print(
            f"[yellow]No {self.command_name} run logs on this machine, and no "
            f"production report for this type. Run the scrapers to find out what "
            f"is failing: `manage.py {self.command_name} --all-councils`.[/yellow]"
        )

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
                scraper_cls = load_scraper(council.council_id, self.command_name)
                if scraper_cls:
                    councils.append(council)
        if self.options["exclude_missing"]:
            missing_councils = {c["code"] for c in self.missing()}
            councils = [
                council
                for council in councils
                if council.council_id not in missing_councils
            ]
        return councils

    @property
    def in_lambda(self):
        """
        Lambda runs one council per invocation through its own handler and
        keeps its logs in CloudWatch, so there is nothing to write locally.
        """
        return bool(
            self.options.get("aws_lambda") or os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
        )

    #: File a council's last run of this scraper type is recorded in,
    #: alongside the data that run produced.
    RUN_LOG_FILE_NAME = "runlog.json"

    def run_log_path(self, scraper) -> Optional[Path]:
        """
        Where to record this council's run, or None if there is nowhere.

        Run logs sit beside the data they describe, in
        data/<COUNCIL>/<Type>/. Only backends that keep data on this
        machine offer a place for them; the GitHub backend writes to a
        clone elsewhere and reports through the dashboard instead.
        """
        council_root = getattr(scraper.storage_backend, "council_root", None)
        if council_root is None:
            return None
        return council_root / self.RUN_LOG_FILE_NAME

    def record_run_log(self, scraper, run_log):
        """
        Record how this council's run went, replacing any previous one.

        Written straight to disk rather than through the storage session,
        because the runs most worth a record are the ones the session
        never commits: a failure resets the session without writing, and a
        run that scrapes nothing skips the commit entirely.

        The scraper's console output is kept only for runs that failed. It
        is what you need to diagnose one, and keeping it for every council
        would bloat every data directory.
        """
        if self.in_lambda:
            return

        path = self.run_log_path(scraper)
        if path is None:
            return

        entry = {
            "council": scraper.council_id,
            "command": self.command_name,
            "status_code": run_log.status_code,
            "start": run_log.start,
            "end": run_log.end,
            "duration": run_log.duration,
            "error": run_log.error,
        }
        if run_log.error and run_log.log:
            entry["log"] = run_log.log

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(entry, indent=2, default=str))

    def run_councils(self):
        self._run_councils(progress=False)

    def run_councils_with_progress(self):
        self._run_councils(progress=True)

    def _run_councils(self, progress=True):
        to_run = self.councils_to_run
        self._council_count = len(to_run)
        workers = max(1, int(self.options.get("workers") or 1))
        # Nothing to parallelise, and one council should stream its output
        # rather than being held back and replayed.
        if self._council_count <= 1:
            workers = 1
        self._concurrent = workers > 1

        if not progress:
            self._map(to_run, workers, None)
        else:
            with Progress(
                "[progress.description]{task.description}",
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn(),
                console=self.console,
                auto_refresh=False,
            ) as bar:
                task = bar.add_task(description="Total", total=self._council_count)
                self._map(to_run, workers, (bar, task))

        if self._concurrent:
            self.output_run_summary()

    def _map(self, councils, workers, progress):
        def done():
            if progress:
                bar, task = progress
                bar.update(task, advance=1)
                bar.refresh()

        if workers == 1:
            for council in councils:
                self.run_council(council.council_id)
                done()
            return

        with ThreadPoolExecutor(workers) as pool:
            futures = [
                pool.submit(self.run_council, council.council_id)
                for council in councils
            ]
            for future in as_completed(futures):
                future.result()
                done()

    def output_run_summary(self):
        """Per-council outcomes, for runs too large to read as they happen."""
        table = Table(title=f"{self.command_name}: {len(self._results)} councils run")
        table.add_column("Council", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Seconds", style="cyan", justify="right")
        table.add_column("Error", style="red", overflow="fold")

        for council_id, run_log in sorted(self._results):
            failed = bool(run_log.error)
            table.add_row(
                council_id,
                "[red]failed[/red]" if failed else "ok",
                f"{run_log.duration.total_seconds():.1f}",
                run_log.error.strip().splitlines()[-1][:120] if failed else "",
            )
        self.console.print(table)

        failures = [c for c, log in self._results if log.error]
        if failures:
            self.console.print(
                f"[red]{len(failures)} of {len(self._results)} failed:[/red] "
                + " ".join(sorted(failures))
            )

    def _run_single(self, scraper, console):
        run_log = settings.RUN_LOGGER(start=datetime.datetime.now(datetime.UTC))
        try:
            scraper.run(run_log)
            if self.options.get("report"):
                with self._lock:
                    self.scraped_items.extend(scraper.report_items)
        except KeyboardInterrupt:
            raise
        except Exception:
            run_log.error = traceback.format_exc()
            if not run_log.log and hasattr(console, "export_text"):
                run_log.log = console.export_text()
            # One council failing must not end a run over many of them.
            # A run of exactly one is someone working on that scraper, or a
            # scripted call that wants the failure to surface.
            if self._council_count <= 1 and self.options.get("verbose"):
                run_log.finish()
                self.record_run_log(scraper, run_log)
                raise
        run_log.finish()

        with self._lock:
            self._results.append((scraper.options["council"], run_log))
        self.record_run_log(scraper, run_log)

        if not self._concurrent:
            console.print(run_log.as_rich_table)

    def run_council(self, council):
        # Each council gets its own options and console: sharing them means
        # concurrent runs overwrite each other's council and interleave
        # their logs into each other's run records.
        options = dict(self.options)
        options["council"] = council
        options["council_info"] = load_council_info(council)

        console = self.console
        if self._concurrent:
            console = Console(file=io.StringIO(), record=True, width=120)

        scraper_cls = load_scraper(council, self.command_name)
        if not scraper_cls:
            return
        with scraper_cls(options, console) as scraper:
            should_run = True
            if scraper.disabled:
                should_run = False
            if should_run and options["refresh"] and scraper.run_since():
                should_run = False
            if should_run and options["tags"]:
                required_tags = set(options["tags"].split(","))
                scraper_tags = set(scraper.get_tags)
                if not required_tags.issubset(scraper_tags):
                    should_run = False
            if should_run:
                # Clear console recording to exclude bootstrapping output from run log
                if hasattr(console, "_record_buffer"):
                    console._record_buffer.clear()
                self._run_single(scraper, console)

    def normalise_codes(self):
        new_codes = []
        if self.options.get("council"):
            old_codes = self.options["council"].split(",")

            for code in old_codes:
                new_codes.append(_abs_path(settings.SCRAPER_DIR_NAME, code)[1])
        self.options["council"] = ",".join(new_codes)
        return self.options

    def output_report(self):
        """Display a report of scraped data. Subclasses render their own table."""
        self.console.print(
            f"\n[bold green]Total items scraped: {len(self.scraped_items)}[/bold green]"
        )

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
        else:
            self.run_councils()

        # Display report if requested
        if options.get("report"):
            self.output_report()

        return None
