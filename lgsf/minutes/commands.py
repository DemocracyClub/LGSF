import csv
import datetime

from rich.table import Table

from lgsf.commands.base import PerCouncilCommandBase


def read_since_file(path):
    """{council code: ISO start date} from a CSV with council and since columns."""
    with open(path, newline="") as f:
        return {
            row["council"].strip().upper(): datetime.date.fromisoformat(
                row["since"].strip()
            ).isoformat()
            for row in csv.DictReader(f)
            if row.get("council") and row.get("since")
        }


class Command(PerCouncilCommandBase):
    command_name = "minutes"

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-documents",
            action="store_true",
            help="Find and record documents but don't download them. Useful "
            "for checking a scraper works without pulling every PDF.",
        )
        parser.add_argument(
            "--since",
            type=lambda value: datetime.date.fromisoformat(value).isoformat(),
            help="Scrape meetings from this date (YYYY-MM-DD) rather than "
            "the last few weeks. For a backfill; it can be stopped and rerun. "
            "CMIS councils list one month per request, about 40 seconds each, "
            "before any meeting is saved.",
        )
        parser.add_argument(
            "--since-file",
            type=read_since_file,
            help="A CSV with council and since columns giving each council's "
            "own start date, as scripts/poteris_since.py writes. A council not "
            "in it starts at --since, or the last few weeks.",
        )

    def output_report(self):
        """Display a Rich table report of scraped meeting data"""
        table = Table(title="Scraped Meetings")
        table.add_column("Date", style="cyan", no_wrap=True)
        table.add_column("Committee", style="green", no_wrap=False)
        table.add_column("Status", style="yellow", no_wrap=False)
        table.add_column("Minutes", style="blue", no_wrap=True)
        table.add_column("Documents", style="magenta", no_wrap=True)

        for meeting in sorted(self.scraped_items, key=lambda m: m.date):
            documents = getattr(meeting, "documents", []) or []
            minutes_docs = [d for d in documents if d.get("category") == "minutes"]
            stored = [d for d in documents if d.get("storage_key")]
            table.add_row(
                meeting.date or "N/A",
                meeting.committee or "N/A",
                getattr(meeting, "status", None) or "",
                "yes" if getattr(meeting, "minutes_published", False) else "",
                f"{len(stored)} stored / {len(documents)} linked "
                f"({len(minutes_docs)} minutes)",
            )

        total_meetings = len(self.scraped_items)
        if total_meetings > 0:
            self.console.print(
                f"\n[bold green]Total meetings scraped: {total_meetings}[/bold green]"
            )
            self.console.print(table)
        else:
            self.console.print("[yellow]No meeting data found to report[/yellow]")
