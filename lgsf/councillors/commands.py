from rich.table import Table

from lgsf.commands.aws_mixin import AWSInvokableMixin
from lgsf.commands.base import PerCouncilCommandBase


class Command(AWSInvokableMixin, PerCouncilCommandBase):
    command_name = "councillors"

    # The scheduled production job behind this dashboard runs councillors
    # scrapers.
    failing_api_url = "https://democracyclub.github.io/lgsf-dashboard/api/failing.json"

    def output_report(self):
        """Display a Rich table report of scraped councillor data"""
        table = Table(title="Scraped Councillor Data")
        table.add_column("Name", style="cyan", no_wrap=False)
        table.add_column("Ward", style="green", no_wrap=False)
        table.add_column("Party", style="yellow", no_wrap=False)
        table.add_column("Email", style="blue", no_wrap=False)
        table.add_column("Photo", style="magenta", overflow="fold")

        # Use councillors collected during scraping (in memory)
        for councillor in sorted(self.scraped_items, key=lambda c: c.name):
            table.add_row(
                councillor.name or "N/A",
                councillor.division or "N/A",
                councillor.party or "N/A",
                getattr(councillor, "email", None) or "",
                getattr(councillor, "photo_url", None) or "",
            )

        total_councillors = len(self.scraped_items)
        if total_councillors > 0:
            self.console.print(
                f"\n[bold green]Total councillors scraped: {total_councillors}[/bold green]"
            )
            self.console.print(table)
        else:
            self.console.print("[yellow]No councillor data found to report[/yellow]")
