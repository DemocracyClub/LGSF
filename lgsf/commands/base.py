import abc
import argparse
import os

from lgsf.conf import settings
from lgsf.path_utils import _abs_path


class CommandBase(metaclass=abc.ABCMeta):
    def __init__(self, argv):
        self.argv = argv
        self.create_parser()
        self.execute()

    def create_parser(self):
        self.parser = argparse.ArgumentParser()
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
        if not any((args.council, args.all_councils, args.tags)):
            self.parser.error(
                "one of --council or --all-councils or --tags required"
            )
        if args.council and args.tags:
            self.parser.error("Can't use --tags and --council together")
        return args

    def councils_to_run(self):
        councils = []
        if self.options["all_councils"] or self.options["tags"]:
            councils = [
                d.split("-")[0]
                for d in os.listdir(settings.SCRAPER_DIR_NAME)
                if os.path.isdir(os.path.join(settings.SCRAPER_DIR_NAME, d))
                and not d.startswith("__")
            ]

        else:
            for council in self.options["council"].split(","):
                council = council.strip().split("-")[0].upper()
                councils.append(council)
        return councils

    def normalise_codes(self):
        if self.options.get('council'):
            old_codes = self.options["council"].split(",")
            new_codes = []
            for code in old_codes:
                new_codes.append(_abs_path(settings.SCRAPER_DIR_NAME, code)[1])
        self.options['council'] = ",".join(new_codes)
        return self.options
