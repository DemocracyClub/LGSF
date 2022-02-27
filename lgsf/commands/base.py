import abc
import argparse
import os
import json

from dateutil.parser import parse
from dateutil.utils import today
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeElapsedColumn
from rich.table import Table

from lgsf.conf import settings
from lgsf.path_utils import _abs_path, load_scraper, load_council_info


class CommandBase(metaclass=abc.ABCMeta):
    command_name = None

    def __init__(self, argv, stdout, pretty=False):
        self.argv = argv
        self.create_parser()
        self.stdout = stdout
        self.console = Console(file=self.stdout)
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

    def execute(self):
        self.options = vars(self.create_parser())
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


class PerCouncilCommandBase(CommandBase):
    """
    For commands that operate on a list of councils
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

        self.add_default_arguments(self.parser)

        if hasattr(self, "add_arguments"):
            self.add_arguments(self.parser)

        args = self.parser.parse_args(self.argv[1:])
        if args.list_missing or args.list_disabled:
            return args
        if not any((args.council, args.all_councils, args.tags)):
            self.parser.error("one of --council or --all-councils or --tags required")
        if args.council and args.tags:
            self.parser.error("Can't use --tags and --council together")
        return args

    @property
    def _all_council_dirs(self):
        return [
            d.split("-")[0]
            for d in os.listdir(settings.SCRAPER_DIR_NAME)
            if os.path.isdir(os.path.join(settings.SCRAPER_DIR_NAME, d))
            and not d.startswith("__")
        ]

    def missing(self):
        missing_councils = []
        for council in self._all_council_dirs:
            scraper = load_scraper(council, self.command_name)
            if not scraper:
                metadata_path = os.path.join(
                    _abs_path(settings.SCRAPER_DIR_NAME, council)[0],
                    "metadata.json",
                )
                council_metadata = json.load(open(metadata_path))
                if council_metadata["end_date"]:
                    # This council has a known end data, check if it's in the past
                    if parse(council_metadata["end_date"]) < today():
                        continue
                if parse(council_metadata["start_date"]) > today():
                    continue
                council_info = {
                    "code": council,
                    "name": council_metadata["official_name"],
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

    def disabled(self):
        disabled_councils = []
        for council in self._all_council_dirs:
            scraper = load_scraper(council, self.command_name)
            if scraper and scraper.disabled:
                metadata_path = os.path.join(
                    _abs_path(settings.SCRAPER_DIR_NAME, council)[0],
                    "metadata.json",
                )
                council_metadata = json.load(open(metadata_path))
                council_info = {
                    "code": council,
                    "name": council_metadata["official_name"],
                }
                disabled_councils.append(council_info)
        return sorted(disabled_councils, key=lambda d: d["code"])

    def output_disabled(self):
        table = Table(title=f"Councils with '{self.command_name}' disabled scraper")

        table.add_column("Code", style="magenta")
        table.add_column("Name", style="green")
        for council in self.disabled():
            table.add_row(council["code"], council["name"])

        self.console.print(table)

    def output_status(self):
        from rich.columns import Columns
        from rich.panel import Panel

        missing = str(len(self.missing()))
        disabled = str(len(self.disabled()))
        self.console.print(
            Columns(
                [Panel(disabled, title="Disabled"), Panel(missing, title="Missing")]
            )
        )

    @property
    def councils_to_run(self):
        councils = []
        if self.options["all_councils"] or self.options["tags"]:
            councils = self._all_council_dirs

        else:
            for council in self.options["council"].split(","):
                council = council.strip().split("-")[0].upper()
                councils.append(council)

        if self.options["exclude_missing"]:
            councils = list(set(councils) - set(c["code"] for c in self.missing()))
        return councils

    def run_councils(self):
        for council in self.councils_to_run:
            self.run_council(council)

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
            total = progress.add_task(description=f"Total", total=len(to_run))
            while not progress.finished:
                for council in to_run:
                    self.run_council(council)
                    progress.update(total, advance=1)
                    progress.refresh()

    def _run_single(self, scraper):
        try:
            scraper.run()
        except KeyboardInterrupt:
            raise
        except:
            if self.options.get("verbose"):
                raise

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
            if should_run and self.options["refresh"]:
                if scraper.run_since():
                    should_run = False
            if should_run and self.options["tags"]:
                required_tags = set(self.options["tags"].split(","))
                scraper_tags = set(scraper.get_tags)
                if not required_tags.issubset(scraper_tags):
                    should_run = False
            if should_run:
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
            self.output_missing()

        if options["list_disabled"]:
            self.output_disabled()

        self.output_status()
        self.normalise_codes()
        if self.pretty:
            self.run_councils_with_progress()
        else:
            self.run_councils()
