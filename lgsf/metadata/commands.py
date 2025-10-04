import csv
import json
import sys
from pathlib import Path

import requests
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from lgsf.commands.base import CouncilFilteringCommandBase
from lgsf.metadata.models import CouncilMetadata
from lgsf.path_utils import create_org_package, scraper_abs_path


class Command(CouncilFilteringCommandBase):
    command_name = "metadata"

    def _add_council_filtering_args(self, parser, include_disabled_options=True):
        """Helper to add common council filtering arguments."""
        parser.add_argument(
            "--council",
            action="store",
            help="Filter to specific council(s) - comma separated",
        )
        parser.add_argument(
            "--all-councils",
            action="store_true",
            help="Include all councils (default)",
        )
        if include_disabled_options:
            parser.add_argument(
                "--list-disabled",
                action="store_true",
                help="Include only disabled councils",
            )
            parser.add_argument(
                "--exclude-disabled",
                action="store_true",
                help="Exclude disabled councils",
            )
        parser.add_argument(
            "-t",
            "--tags",
            action="store",
            help="Only include scrapers with the given tags (comma separated)",
        )

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest="subcommand", help="Available commands")

        # Update subcommand
        update_parser = subparsers.add_parser(
            "update", help="Update EveryElection metadata from Democracy Club"
        )
        update_parser.add_argument(
            "--council",
            action="store",
            help="Update metadata for a specific council only",
        )

        # List CMS subcommand
        list_cms_parser = subparsers.add_parser(
            "list-cms", help="List all councils and their CMS types"
        )
        list_cms_parser.add_argument(
            "--service",
            action="store",
            default="councillors",
            help="Service to filter by (default: councillors)",
        )
        list_cms_parser.add_argument(
            "--csv",
            action="store_true",
            help="Output in CSV format",
        )
        list_cms_parser.add_argument(
            "--json",
            action="store_true",
            help="Output in JSON format",
        )
        self._add_council_filtering_args(list_cms_parser)

        # Validate subcommand
        validate_parser = subparsers.add_parser(
            "validate", help="Validate scrapers against their metadata"
        )
        self._add_council_filtering_args(validate_parser)

    def handle(self, options):
        self.options = options  # Store options for use in filtering
        subcommand = options.get("subcommand")

        if subcommand == "list-cms":
            self.list_cms_types(
                options.get("service", "councillors"),
                csv_format=options.get("csv", False),
                json_format=options.get("json", False),
            )
            return

        elif subcommand == "validate":
            if options.get("council"):
                council_ids = [c.strip() for c in options.get("council").split(",")]
                if len(council_ids) == 1:
                    self.validate_single_scraper(council_ids[0])
                else:
                    self.validate_multiple_scrapers(council_ids)
            else:
                self.validate_all_scrapers()
            return

        elif subcommand == "update":
            if options.get("council"):
                self.update_single_council(options.get("council"))
            else:
                self.update_all_councils()
            return

        # Show help if no subcommand provided
        self.console.print(
            "[yellow]Please specify a subcommand. Use --help to see available options.[/yellow]"
        )

    def update_all_councils(self):
        """Update EveryElection metadata for all councils."""
        base_url = "https://elections.democracyclub.org.uk/"
        url = f"{base_url}api/organisations/"

        councils_updated = 0

        while url:
            req = requests.get(url)
            data = req.json()
            url = data.get("next")

            for org in data["results"]:
                if org["organisation_type"] == "local-authority":
                    councils_updated += 1
                    self.update_council_metadata(org)

        print(f"Updated EveryElection metadata for {councils_updated} councils")

    def update_single_council(self, council_id):
        """Update EveryElection metadata for a single council."""
        base_url = "https://elections.democracyclub.org.uk/"
        url = f"{base_url}api/organisations/"

        # Search for the specific council
        params = {"official_identifier": council_id}
        req = requests.get(url, params=params)
        data = req.json()

        found = False
        for org in data["results"]:
            if (
                org["organisation_type"] == "local-authority"
                and org["official_identifier"] == council_id
            ):
                self.update_council_metadata(org)
                found = True
                print(f"Updated EveryElection metadata for {council_id}")
                break

        if not found:
            print(f"Council {council_id} not found in EveryElection API")

    def update_council_metadata(self, org_data):
        """Update metadata for a single council, preserving manual data."""
        council_id = org_data["official_identifier"]
        print(f"Updating metadata for {council_id}")

        try:
            # Try to get existing scraper path
            scraper_path = scraper_abs_path(council_id)
        except IOError:
            # Create new org package if it doesn't exist
            name = f"{council_id}-{org_data['slug']}"
            scraper_path = create_org_package(name)

        metadata_file = scraper_path / "metadata.json"

        # Load existing metadata or create new
        metadata = CouncilMetadata.from_file(metadata_file)

        # Update EveryElection data while preserving manual data
        metadata.update_everyelectiion_data(org_data)

        # Auto-detect CMS and scraper info if not already set
        self.auto_update_scraper_info(metadata, scraper_path)

        # Save updated metadata
        metadata.save_to_file(metadata_file)

        # Ensure __init__.py exists
        init_file = scraper_path / "__init__.py"
        if not init_file.exists():
            init_file.touch()

    def auto_update_scraper_info(self, metadata: CouncilMetadata, scraper_path: Path):
        """Auto-detect and update scraper information if not manually set."""
        councillors_file = scraper_path / "councillors.py"

        councillors_service = metadata.get_councillors_service()

        if councillors_file.exists():
            try:
                # Read the scraper file to extract base_url if not set
                with open(councillors_file, "r") as f:
                    content = f.read()

                import re

                updates = {}

                # Extract base_url if not set
                if not councillors_service.base_url:
                    url_match = re.search(
                        r'base_url\s*=\s*["\']([^"\']+)["\']', content
                    )
                    if url_match:
                        updates["base_url"] = url_match.group(1)

                if updates:
                    metadata.update_service_data("councillors", **updates)

            except Exception as e:
                print(
                    f"Warning: Could not auto-detect scraper info for {scraper_path.name}: {e}"
                )

    def update_manual_metadata(self, options):
        """Update manual metadata fields for a specific council."""
        council_id = options["council"]

        try:
            scraper_path = scraper_abs_path(council_id)
        except IOError:
            print(f"Council {council_id} not found. Run with --update first.")
            return

        metadata_file = scraper_path / "metadata.json"
        metadata = CouncilMetadata.from_file(metadata_file)

        updates = {}

        if updates:
            metadata.update_service_data("councillors", **updates)
            metadata.save_to_file(metadata_file)
            print(f"Updated councillors service metadata for {council_id}")
            print("Changes made:")
            for key, value in updates.items():
                print(f"  {key}: {value}")
        else:
            print("No updates specified")

    def list_cms_types(
        self, service_name="councillors", csv_format=False, json_format=False
    ):
        """List all councils and their CMS types for a specific service."""
        # Validate format options
        if csv_format and json_format:
            self.console.print(
                "[red]Error: Cannot specify both --csv and --json formats[/red]"
            )
            return
        councils_data = []
        available_services = set()

        # Use the existing council infrastructure with filtering
        councils_to_check = self._get_filtered_councils()

        for council in councils_to_check:
            try:
                metadata = CouncilMetadata.for_council(council.council_id)

                # Track available services across all councils
                if service_name == "councillors" and metadata.councillors.to_dict():
                    available_services.add("councillors")
                # Future services can be added here
                # if service_name == "meetings" and metadata.meetings.to_dict():
                #     available_services.add("meetings")

                service_metadata = metadata.get_service_metadata(service_name)

                # Only include councils that have the requested service
                if service_metadata and service_metadata.to_dict():
                    summary = {
                        "official_identifier": metadata.everyelection_data.official_identifier
                        or council.council_id,
                        "common_name": metadata.everyelection_data.common_name
                        or council.metadata.get("official_name", "Unknown"),
                        "service_name": service_name,
                        "cms_type": service_metadata.cms_type,
                        "base_url": service_metadata.base_url,
                    }
                    councils_data.append(summary)
            except Exception:
                # Skip councils that can't be loaded
                continue

        # If no data found for the service, show helpful message
        if not councils_data:
            self.console.print(
                f"[yellow]No councils found with '{service_name}' service data.[/yellow]"
            )
            if available_services:
                services_list = ", ".join(sorted(available_services))
                self.console.print(f"Available services: {services_list}")
            return

        # Sort by council ID (handle None values)
        councils_data.sort(key=lambda x: x.get("official_identifier") or "")

        # Handle output formats
        if json_format:
            self._output_json(councils_data, service_name)
            return
        elif csv_format:
            self._output_csv(councils_data, service_name)
            return

        # Create Rich table for councils
        table = Table(title=f"Council CMS Summary - {service_name.title()} Service")
        table.add_column("ID", style="cyan", width=6)
        table.add_column("Name", style="green", width=30)
        table.add_column("Service", style="magenta", width=10)
        table.add_column("CMS", style="yellow", width=12)
        table.add_column("Base URL", style="blue", overflow="ellipsis")

        for council in councils_data:
            table.add_row(
                council.get("official_identifier") or "N/A",
                (council.get("common_name") or "N/A")[:28],
                council.get("service_name") or "N/A",
                council.get("cms_type") or "Unknown",
                council.get("base_url") or "N/A",
            )

        self.console.print(table)
        self.console.print(
            f"[dim]Showing {len(councils_data)} councils with {service_name} service data[/dim]"
        )

        # Create CMS statistics
        cms_counts = {}
        for council in councils_data:
            cms_type = council.get("cms_type", "Unknown")
            cms_counts[cms_type] = cms_counts.get(cms_type, 0) + 1

        # Create Rich table for statistics
        stats_table = Table(
            title=f"CMS Type Statistics - {service_name.title()} Service"
        )
        stats_table.add_column("CMS Type", style="cyan")
        stats_table.add_column("Count", style="green", justify="right")
        stats_table.add_column("Percentage", style="yellow", justify="right")

        total = len(councils_data)
        for cms_type, count in sorted(
            cms_counts.items(), key=lambda x: x[1], reverse=True
        ):
            percentage = f"{(count / total * 100):.1f}%"
            stats_table.add_row(cms_type or "Unknown", str(count), percentage)

        self.console.print()
        self.console.print(stats_table)

    def _output_json(self, councils_data, service_name):
        """Output councils data in JSON format."""
        output = {
            "service": service_name,
            "councils": councils_data,
            "total_count": len(councils_data),
        }
        json.dump(output, sys.stdout, indent=2)
        sys.stdout.write("\n")

    def _output_csv(self, councils_data, service_name):
        """Output councils data in CSV format."""
        if not councils_data:
            return

        fieldnames = [
            "official_identifier",
            "common_name",
            "service_name",
            "cms_type",
            "base_url",
        ]
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()

        for council in councils_data:
            writer.writerow(
                {
                    "official_identifier": council.get("official_identifier") or "",
                    "common_name": council.get("common_name") or "",
                    "service_name": council.get("service_name") or "",
                    "cms_type": council.get("cms_type") or "",
                    "base_url": council.get("base_url") or "",
                }
            )

    def _get_filtered_councils(self):
        """Get councils filtered by the standard PerCouncilCommandBase options."""
        if hasattr(self, "options") and self.options:
            # Handle specific council filtering
            if self.options.get("council"):
                council_ids = [
                    c.strip() for c in self.options.get("council").split(",")
                ]
                return [c for c in self.all_councils if c.council_id in council_ids]

            # Handle disabled filtering with safe access
            if self.options.get("list_disabled"):
                try:
                    return [
                        c
                        for c in self.all_councils
                        if c.council_id in [d["code"] for d in self.disabled()]
                    ]
                except Exception:
                    # Fallback if disabled() fails
                    return []
            elif self.options.get("exclude_disabled"):
                try:
                    disabled_codes = {d["code"] for d in self.disabled()}
                    return [
                        c
                        for c in self._safe_current_councils()
                        if c.council_id not in disabled_codes
                    ]
                except Exception:
                    # Fallback if disabled() fails
                    return self._safe_current_councils()

            # Handle tag filtering
            if self.options.get("tags"):
                from lgsf.path_utils import load_scraper

                tag_list = [t.strip() for t in self.options.get("tags").split(",")]
                filtered_councils = []
                for council in self._safe_current_councils():
                    try:
                        scraper = load_scraper(council.council_id, "councillors")
                        if scraper and hasattr(scraper, "tags"):
                            if any(tag in scraper.tags for tag in tag_list):
                                filtered_councils.append(council)
                    except Exception:
                        continue
                return filtered_councils

        # Default to safe current councils
        return self._safe_current_councils()

    def _safe_current_councils(self):
        """Get current councils with safe metadata access."""
        safe_councils = []
        for council in self.all_councils:
            try:
                # Check if council has required metadata and is current
                if hasattr(council, "current") and council.current:
                    safe_councils.append(council)
            except (KeyError, AttributeError):
                # Include council even if metadata check fails
                safe_councils.append(council)
        return safe_councils

    def validate_all_scrapers(self):
        """Validate all scrapers and print a comprehensive report."""
        from lgsf.metadata.validation import ScraperValidator

        validator = ScraperValidator()

        # Get filtered councils
        councils_to_validate = self._get_filtered_councils()

        with self.console.status(
            f"[bold green]Running validation for {len(councils_to_validate)} scrapers..."
        ):
            report = validator.validate_filtered_scrapers(councils_to_validate)

        validator.print_validation_report(report, console=self.console)

    def validate_multiple_scrapers(self, council_ids):
        """Validate multiple scrapers and print detailed reports."""
        from lgsf.metadata.validation import ScraperValidator

        validator = ScraperValidator()

        for council_id in council_ids:
            self.console.print(f"\n[bold blue]Validating {council_id}...[/bold blue]")

            with self.console.status(
                f"[bold green]Validating scraper for {council_id}..."
            ):
                report = validator.validate_council_scraper(council_id)

            validator.print_single_council_report(report, console=self.console)

    def validate_single_scraper(self, council_id):
        """Validate a single scraper and print detailed report."""
        from lgsf.metadata.validation import ScraperValidator

        validator = ScraperValidator()

        with self.console.status(f"[bold green]Validating scraper for {council_id}..."):
            report = validator.validate_council_scraper(council_id)

        # Create a panel with the validation status
        if report["valid"]:
            status_text = Text("✓ Scraper validation passed", style="bold green")
        else:
            status_text = Text("✗ Scraper validation failed", style="bold red")

        panel = Panel(status_text, title=f"Validation Report for {council_id}")
        self.console.print(panel)

        # Create table for issues if any exist
        if report["errors"] or report["warnings"] or report["suggestions"]:
            issues_table = Table(title="Issues Found")
            issues_table.add_column("Type", style="cyan", width=12)
            issues_table.add_column("Description", style="white")

            for error in report.get("errors", []):
                issues_table.add_row("Error", f"✗ {error}", style="red")

            for warning in report.get("warnings", []):
                issues_table.add_row("Warning", f"⚠ {warning}", style="yellow")

            for suggestion in report.get("suggestions", []):
                issues_table.add_row("Suggestion", f"💡 {suggestion}", style="blue")

            self.console.print(issues_table)
        else:
            self.console.print("[green]No issues found![/green]")
