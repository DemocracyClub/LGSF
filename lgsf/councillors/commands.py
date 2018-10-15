from lgsf.commands.base import PerCouncilCommandBase
from lgsf.path_utils import load_scraper
from retry import retry


class Command(PerCouncilCommandBase):
    command_name = "councillors"

    def add_arguments(self, parser):
        parser.add_argument(
            "--check-only",
            action="store_true",
            help="Just check for updated pages, don't scrape anything",
        )

    @retry(tries=3, delay=2)
    def _run_single(self, scraper):
        try:
            scraper.run()
        except KeyboardInterrupt:
            raise
        except:
            print(
                "Error running {}, see {} for more".format(
                    self.options["council"], scraper._error_file_name()
                )
            )

    def handle(self, options):
        self.options = options
        for council in self.councils_to_run():
            self.options["council"] = council
            scraper_cls = load_scraper(council, self.command_name)
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
