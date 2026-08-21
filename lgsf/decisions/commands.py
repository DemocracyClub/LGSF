from rich.table import Table

from lgsf.commands.base import PerCouncilCommandBase


class Command(PerCouncilCommandBase):
    command_name = "decisions"

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-documents",
            action="store_true",
            help="Find and record documents but don't download them. Useful "
            "for checking a scraper works without pulling every PDF.",
        )

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
