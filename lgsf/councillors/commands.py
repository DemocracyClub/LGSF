from lgsf.commands.base import PerCouncilCommandBase
from lgsf.path_utils import load_scraper, load_council_info
from retry import retry


class Command(PerCouncilCommandBase):
    command_name = "councillors"

    def add_arguments(self, parser):
        parser.add_argument(
            "--check-only",
            action="store_true",
            help="Just check for updated pages, don't scrape anything",
        )
        parser.add_argument(
            "--list-missing",
            action="store_true",
            help="Print missing councils",
        )
        parser.add_argument(
            "--list-disabled",
            action="store_true",
            help="Print disabled councils",
        )

    def _run_single(self, scraper):
        try:

            from lgsf.scrapers.councillors import ModGovCouncillorScraper
            if isinstance(scraper, ModGovCouncillorScraper):

                print("\t".join([scraper.options['council'], scraper.base_url]))

            else:
                print("\t".join([scraper.options['council'], str(scraper.class_tags)]))
            scraper.run()
        except KeyboardInterrupt:
            raise
        except:
            if self.options.get('verbose'):
                raise
            print(
                "Error running {}, see {} for more".format(
                    self.options["council"], scraper._error_file_name()
                )
            )

    def handle(self, options):
        self.options = options
        if options["list_missing"]:
            for council in self.missing(self.command_name):
                print(council)

        if options["list_disabled"]:
            for council in self.disabled(self.command_name):
                print(council)
        self.normalise_codes()
        for council in self.councils_to_run():
            self.options["council"] = council
            self.options["council_info"] = load_council_info(council)
            scraper_cls = load_scraper(council, self.command_name)
            if not scraper_cls:
                continue
            with scraper_cls((self.options)) as scraper:
                should_run = True
                if scraper.disabled:
                    should_run = False

                if should_run and options["refresh"]:
                    if scraper.run_since():
                        should_run = False

                if should_run and options["tags"]:
                    required_tags = set(options["tags"].split(","))
                    scraper_tags = set(scraper.get_tags)
                    if not required_tags.issubset(scraper_tags):
                        should_run = False

                if should_run:
                    if options.get("verbose"):
                        print(council)

                    self._run_single(scraper)
