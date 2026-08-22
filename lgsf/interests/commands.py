from rich.table import Table

from lgsf.commands.base import PerCouncilCommandBase


class Command(PerCouncilCommandBase):
    command_name = "interests"

    def output_report(self):
        """Display a Rich table report of scraped Register of Interests data"""
        table = Table(title="Scraped Registers of Interests")
        table.add_column("Councillor", style="green", no_wrap=False)
        table.add_column("Ward / Division", style="cyan", no_wrap=True)
        table.add_column("Published Date", style="yellow", no_wrap=False)
        table.add_column("Categories Declared", style="magenta", no_wrap=True)
        table.add_column("Documents", style="blue", no_wrap=True)

        for record in sorted(self.scraped_items, key=lambda r: r.councillor_name):
            interests = getattr(record, "interests", []) or []
            documents = getattr(record, "documents", []) or []
            table.add_row(
                record.councillor_name or "N/A",
                getattr(record, "division", "") or "N/A",
                getattr(record, "published_date", "") or "N/A",
                f"{len(interests)} categories",
                f"{len(documents)} linked",
            )

        total_records = len(self.scraped_items)
        if total_records > 0:
            self.console.print(
                f"\n[bold green]Total registers of interests scraped: {total_records}[/bold green]"
            )
            self.console.print(table)
        else:
            self.console.print(
                "[yellow]No register of interests data found to report[/yellow]"
            )
