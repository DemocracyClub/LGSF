import abc
import argparse
import os
import json

from lgsf.conf import settings
from lgsf.path_utils import _abs_path, load_scraper


class CommandBase(metaclass=abc.ABCMeta):
    def __init__(self, argv):
        self.argv = argv
        self.create_parser()
        self.execute()

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
        self.add_default_arguments(self.parser)
        self.add_arguments(self.parser)
        args = self.parser.parse_args(self.argv[1:])
        if args.list_missing or args.list_disabled:
            return args
        if not any((args.council, args.all_councils, args.tags)):
            self.parser.error(
                "one of --council or --all-councils or --tags required"
            )
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

    def missing(self, command_name):
        missing_councils = []
        for council in self._all_council_dirs:
            scraper = load_scraper(council, command_name)
            if not scraper:
                metadata_path = os.path.join(
                    _abs_path(settings.SCRAPER_DIR_NAME, council)[0],
                    "metadata.json",
                )
                council_metadata = json.load(open(metadata_path))
                nice_name = "{} - {}".format(council, council_metadata["slug"])
                missing_councils.append(nice_name)
        return sorted(missing_councils)

    def disabled(self, command_name):
        disabled_councils = []
        for council in self._all_council_dirs:
            scraper = load_scraper(council, command_name)
            if scraper and scraper.disabled:
                metadata_path = os.path.join(
                    _abs_path(settings.SCRAPER_DIR_NAME, council)[0],
                    "metadata.json",
                )
                council_metadata = json.load(open(metadata_path))
                nice_name = "{} - {}".format(council, council_metadata["slug"])
                disabled_councils.append(nice_name)
        return sorted(disabled_councils)

    def councils_to_run(self):
        councils = []
        if self.options["all_councils"] or self.options["tags"]:
            councils = self._all_council_dirs

        else:
            for council in self.options["council"].split(","):
                council = council.strip().split("-")[0].upper()
                councils.append(council)
        return councils

    def normalise_codes(self):
        new_codes = []
        if self.options.get("council"):
            old_codes = self.options["council"].split(",")

            for code in old_codes:
                new_codes.append(_abs_path(settings.SCRAPER_DIR_NAME, code)[1])
        self.options["council"] = ",".join(new_codes)
        return self.options
