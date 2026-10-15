import datetime
from concurrent.futures import ThreadPoolExecutor

from dateutil import parser as date_parser
from rich.table import Table

from lgsf.commands.base import PerCouncilCommandBase
from lgsf.path_utils import load_scraper
from lgsf.storage.backends import get_storage_backend


class Command(PerCouncilCommandBase):
    command_name = "decisions"

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-documents",
            action="store_true",
            help="Find and record documents but don't download them. Useful "
            "for checking a scraper works without pulling every PDF.",
        )
        since = parser.add_mutually_exclusive_group()
        since.add_argument(
            "--since",
            type=lambda value: datetime.date.fromisoformat(value).isoformat(),
            help="Scrape decisions published since this date (YYYY-MM-DD) "
            "rather than the default year. Use for a backfill: slices of it "
            "already read completely are skipped, so it can be stopped and "
            "rerun.",
        )
        since.add_argument(
            "--discover-since",
            action="store_true",
            help="Like --since, from the oldest decision each council lists. "
            "Found once per council and remembered.",
        )
        parser.add_argument(
            "--request-interval",
            type=float,
            help="Minimum seconds between requests to one host. Overrides "
            "the scraper's request_interval.",
        )

    @property
    def councils_to_run(self):
        councils = super().councils_to_run
        if len(councils) <= 1:
            return councils
        return self.backfill_order(councils)

    #: How many councils' stored state to look up at once.
    lookup_workers = 16

    def backfill_order(self, councils):
        """
        Councils with nothing stored first, then those stopped part way
        through (an index but no completed run), then the rest by when their
        last run completed, oldest first.

        A backfill takes longer than credentials last, so it is stopped and
        resumed. In this order a resume starts on what hasn't been touched
        rather than listing again councils it has just finished, which come
        last. Add --refresh HOURS to skip those altogether.

        Councils with no decisions scraper have nothing to run and go last.
        """
        # Loaded here rather than in the pool: every scraper module is
        # loaded under the same name, so loading them in threads can mix
        # them up.
        scrapers = [load_scraper(c.council_id, self.command_name) for c in councils]
        to_look_up = [(c, cls) for c, cls in zip(councils, scrapers) if cls]
        with ThreadPoolExecutor(self.lookup_workers) as pool:
            states = list(pool.map(lambda args: self.stored_state(*args), to_look_up))

        def key(item):
            has_data, last_run = item[0]
            return (has_data, last_run is not None, last_run or datetime.datetime.min)

        ordered = sorted(
            zip(states, (c for c, _ in to_look_up)),
            key=key,
        )
        empty = sum(1 for (has_data, _), _ in ordered if not has_data)
        self.console.print(
            f"{empty} of {len(ordered)} councils have no decisions stored and "
            f"run first; the rest follow, least recently completed first"
        )
        without_scraper = [c for c, cls in zip(councils, scrapers) if not cls]
        return [council for _, council in ordered] + without_scraper

    def stored_state(self, council, scraper_cls):
        """
        (whether anything is stored, when a run last completed) for one
        council, read without starting a storage session: a session lists
        every stored key on S3, and creates directories locally.
        """
        backend = get_storage_backend(
            council_code=council.council_id,
            options=self.options,
            scraper_object_type=scraper_cls.scraper_object_type,
            storage_mode=scraper_cls.storage_mode,
        )
        has_index = self.stored_exists(backend, scraper_cls.INDEX_FILE_NAME)
        last_run = self.stored_read(backend, scraper_cls.LAST_RUN_FILE_NAME)
        if last_run:
            try:
                last_run = date_parser.parse(last_run).replace(tzinfo=None)
            except (ValueError, OverflowError):
                last_run = None
        return has_index or bool(last_run), last_run or None

    def stored_exists(self, backend, name):
        root = getattr(backend, "council_root", None)
        if root is not None:
            return (root / name).is_file()
        exists_now = getattr(backend, "exists_now", None)
        # Backends that can't say, such as GitHub, are treated as holding
        # something, which leaves their order alone.
        return exists_now(name) if exists_now else True

    def stored_read(self, backend, name):
        root = getattr(backend, "council_root", None)
        if root is not None:
            path = root / name
            return path.read_text() if path.is_file() else None
        read_now = getattr(backend, "read_now", None)
        return read_now(name) if read_now else None

    def output_report(self):
        """Display a Rich table report of scraped decision data"""
        table = Table(title="Scraped Decisions")
        table.add_column("Date", style="cyan", no_wrap=True)
        table.add_column("Title", style="green", no_wrap=False)
        table.add_column("Status", style="yellow", no_wrap=False)
        table.add_column("Key", style="blue", no_wrap=True)
        table.add_column("Documents", style="magenta", no_wrap=True)

        for decision in sorted(self.scraped_items, key=lambda d: d.date):
            documents = getattr(decision, "documents", []) or []
            stored = [d for d in documents if d.get("storage_key")]
            table.add_row(
                decision.date or "N/A",
                decision.title or "N/A",
                getattr(decision, "status", None) or "",
                "yes" if getattr(decision, "is_key_decision", False) else "",
                f"{len(stored)} stored / {len(documents)} linked",
            )

        total_decisions = len(self.scraped_items)
        if total_decisions > 0:
            self.console.print(
                f"\n[bold green]Total decisions scraped: {total_decisions}[/bold green]"
            )
            self.console.print(table)
        else:
            self.console.print("[yellow]No decision data found to report[/yellow]")
