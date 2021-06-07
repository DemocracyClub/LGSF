from rich.progress import Progress

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

    def _run_single(self, scraper, progress, tasks):
        try:

            from lgsf.scrapers.councillors import ModGovCouncillorScraper

            if isinstance(scraper, ModGovCouncillorScraper):

                progress.console.print(
                    "\t".join([scraper.options["council"], scraper.base_url])
                )

            else:
                progress.console.print(
                    "\t".join([scraper.options["council"], str(scraper.class_tags)])
                )
            scraper.run()
            progress.update(tasks["completed"], advance=1)
        except KeyboardInterrupt:
            raise
        except:
            if self.options.get("verbose"):
                raise
            progress.update(tasks["failed"], advance=1)
            progress.console.print(
                "Error running asdasd {}, see {} for more".format(
                    self.options["council"], scraper._error_file_name()
                ),
                style="red",
            )


    def handle(self, options):
        self.options = options
        if options["list_missing"]:
            self.output_missing()

        if options["list_disabled"]:
            self.output_disabled()

        self.output_status()

        self.normalise_codes()
        to_run = self.councils_to_run()
        with Progress(
            auto_refresh=False, redirect_stderr=False, redirect_stdout=False
        ) as progress:

            tasks = {
                "total": progress.add_task(description=f"Total", total=len(to_run)),
                "completed": progress.add_task(
                    description=f"Completed", total=len(to_run)
                ),
                "failed": progress.add_task(description=f"Failed", total=len(to_run)),
                "skipped": progress.add_task(description=f"Skipped", total=len(to_run)),
            }

            while not progress.finished:
                for council in to_run:
                    self.options["council"] = council
                    self.options["council_info"] = load_council_info(council)
                    scraper_cls = load_scraper(council, self.command_name)
                    if not scraper_cls:
                        continue
                    with scraper_cls((self.options), progress.console) as scraper:
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
                                progress.console.print(council)

                            self._run_single(scraper, progress, tasks)
                            progress.update(tasks["total"], advance=1)
                        else:
                            progress.update(tasks["skipped"], advance=1)
                            progress.update(tasks["total"], advance=1)
                        progress.refresh()
