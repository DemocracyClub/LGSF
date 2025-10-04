import warnings
from pathlib import Path
from typing import List, Dict, Any, Optional
import importlib.util
import sys

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.columns import Columns

from lgsf.metadata.models import CouncilMetadata
from lgsf.path_utils import scraper_abs_path


class ScraperValidator:
    """
    Utility class for validating scraper implementations against metadata
    and detecting inconsistencies.
    """

    def __init__(self):
        self.warnings_issued = []

    def validate_council_scraper(self, council_id: str) -> Dict[str, Any]:
        """
        Validate a council's scraper against its metadata.

        Returns a validation report with any issues found.
        """
        report = {
            "council_id": council_id,
            "valid": True,
            "warnings": [],
            "errors": [],
            "suggestions": [],
        }

        try:
            # Load metadata
            metadata = CouncilMetadata.for_council(council_id)

            # Check if scraper file exists
            scraper_path = scraper_abs_path(council_id)
            councillors_file = scraper_path / "councillors.py"

            if not councillors_file.exists():
                report["errors"].append("No councillors.py file found")
                report["valid"] = False
                return report

            # Load and inspect the scraper
            scraper_info = self._load_scraper_info(councillors_file)

            if not scraper_info:
                report["errors"].append("Could not load scraper class information")
                report["valid"] = False
                return report

            # Basic metadata checks
            councillors_service = metadata.get_service_metadata("councillors")
            if councillors_service and councillors_service.cms_type:
                # Could add runtime CMS validation here if needed
                pass

            # Check for missing metadata fields
            missing_fields = self._check_missing_metadata(metadata)
            if missing_fields:
                report["suggestions"].extend(
                    [f"Consider setting {field}" for field in missing_fields]
                )

            # Check scraper implementation quality
            quality_issues = self._check_scraper_quality(scraper_info, councillors_file)
            report["warnings"].extend(quality_issues)

            # Check base_url consistency
            councillors_service = metadata.get_service_metadata("councillors")
            if (
                councillors_service
                and councillors_service.base_url
                and scraper_info.get("base_url")
            ):
                if councillors_service.base_url != scraper_info["base_url"]:
                    report["warnings"].append(
                        f"Base URL mismatch: metadata has '{councillors_service.base_url}' "
                        f"but scraper has '{scraper_info['base_url']}'"
                    )

        except Exception as e:
            report["errors"].append(f"Validation failed: {str(e)}")
            report["valid"] = False

        return report

    def _load_scraper_info(self, scraper_file: Path) -> Optional[Dict[str, Any]]:
        """Load information about the scraper class."""
        try:
            with open(scraper_file, "r") as f:
                content = f.read()

            import re

            # Extract parent class
            parent_match = re.search(r"class Scraper\((\w+)\):", content)
            if not parent_match:
                return None

            parent_class = parent_match.group(1)

            # Extract base_url
            url_match = re.search(r'base_url\s*=\s*["\']([^"\']+)["\']', content)
            base_url = url_match.group(1) if url_match else None

            # Extract tags if present
            tags_match = re.search(r"tags\s*=\s*\[([^\]]*)\]", content)
            tags = []
            if tags_match:
                tags_content = tags_match.group(1)
                tags = [
                    tag.strip().strip("\"'")
                    for tag in tags_content.split(",")
                    if tag.strip()
                ]

            # Check if disabled
            disabled_match = re.search(r"disabled\s*=\s*(True|False)", content)
            disabled = disabled_match.group(1) == "True" if disabled_match else False

            # Count methods
            methods = re.findall(r"def (\w+)\(", content)
            custom_methods = [m for m in methods if not m.startswith("_")]

            return {
                "parent_class": parent_class,
                "base_url": base_url,
                "tags": tags,
                "disabled": disabled,
                "custom_methods": custom_methods,
                "content": content,
            }

        except Exception as e:
            return None

    def _check_missing_metadata(self, metadata: CouncilMetadata) -> List[str]:
        """Check for important missing metadata fields."""
        missing = []

        councillors_service = metadata.get_service_metadata("councillors")
        if not councillors_service:
            missing.append("councillors service metadata")
            return missing

        if not councillors_service.cms_type:
            missing.append("cms_type")

        if not councillors_service.base_url:
            missing.append("base_url")

        return missing

    def _check_scraper_quality(
        self, scraper_info: Dict[str, Any], scraper_file: Path
    ) -> List[str]:
        """Check for common scraper implementation issues."""
        issues = []
        content = scraper_info["content"]

        # Check for common issues
        if "TODO" in content.upper():
            issues.append("Scraper contains TODO comments")

        if "FIXME" in content.upper():
            issues.append("Scraper contains FIXME comments")

        if scraper_info["disabled"]:
            issues.append("Scraper is marked as disabled")

        # Check for missing base_url
        if not scraper_info["base_url"]:
            issues.append("No base_url defined in scraper")

        # Check if it's just inheriting without customization
        if (
            scraper_info["parent_class"]
            in ["CMISCouncillorScraper", "ModGovCouncillorScraper"]
            and len(scraper_info["custom_methods"]) == 0
        ):
            # This is actually good - simple CMS scrapers should be minimal
            pass
        elif len(scraper_info["custom_methods"]) == 0:
            issues.append("No custom methods defined - scraper may be incomplete")

        return issues

    def validate_all_scrapers(self) -> Dict[str, Any]:
        """Validate all scrapers and return a comprehensive report."""
        from pathlib import Path

        scrapers_dir = Path("scrapers")
        if not scrapers_dir.exists():
            return {"error": "Scrapers directory not found"}

        results = {
            "total_councils": 0,
            "valid_scrapers": 0,
            "scrapers_with_warnings": 0,
            "scrapers_with_errors": 0,
            "councils": [],
            "summary": {
                "cms_types": {},
                "common_issues": {},
            },
        }

        for council_dir in scrapers_dir.iterdir():
            if council_dir.is_dir() and not council_dir.name.startswith("."):
                council_id = self._extract_council_id(council_dir.name)
                if council_id:
                    results["total_councils"] += 1

                    validation_result = self.validate_council_scraper(council_id)
                    results["councils"].append(validation_result)

                    if validation_result["valid"]:
                        results["valid_scrapers"] += 1

                    if validation_result["warnings"]:
                        results["scrapers_with_warnings"] += 1

                    if validation_result["errors"]:
                        results["scrapers_with_errors"] += 1

                    # Update summary statistics
                    try:
                        metadata = CouncilMetadata.for_council(council_id)
                        councillors_service = metadata.get_service_metadata(
                            "councillors"
                        )

                        # CMS type distribution
                        cms_type = (
                            councillors_service.cms_type
                            if councillors_service
                            else None
                        ) or "Unknown"
                        results["summary"]["cms_types"][cms_type] = (
                            results["summary"]["cms_types"].get(cms_type, 0) + 1
                        )

                        # Common issues
                        for warning in validation_result["warnings"]:
                            results["summary"]["common_issues"][warning] = (
                                results["summary"]["common_issues"].get(warning, 0) + 1
                            )

                    except Exception:
                        pass

        return results

    def validate_filtered_scrapers(self, councils) -> Dict[str, Any]:
        """Validate a filtered list of councils and return a comprehensive report."""
        results = {
            "total_councils": 0,
            "valid_scrapers": 0,
            "scrapers_with_warnings": 0,
            "scrapers_with_errors": 0,
            "councils": [],
            "summary": {
                "cms_types": {},
                "common_issues": {},
            },
        }

        for council in councils:
            council_id = council.council_id
            results["total_councils"] += 1

            validation_result = self.validate_council_scraper(council_id)
            results["councils"].append(validation_result)

            if validation_result["valid"]:
                results["valid_scrapers"] += 1

            if validation_result["warnings"]:
                results["scrapers_with_warnings"] += 1

            if validation_result["errors"]:
                results["scrapers_with_errors"] += 1

            # Update summary statistics
            try:
                metadata = CouncilMetadata.for_council(council_id)
                councillors_service = metadata.get_service_metadata("councillors")

                # CMS type distribution
                cms_type = (
                    councillors_service.cms_type if councillors_service else None
                ) or "Unknown"
                results["summary"]["cms_types"][cms_type] = (
                    results["summary"]["cms_types"].get(cms_type, 0) + 1
                )

                # Common issues
                for warning in validation_result["warnings"]:
                    results["summary"]["common_issues"][warning] = (
                        results["summary"]["common_issues"].get(warning, 0) + 1
                    )

            except Exception:
                pass

        return results

    def _extract_council_id(self, directory_name: str) -> Optional[str]:
        """Extract council ID from directory name."""
        parts = directory_name.split("-")
        if len(parts) >= 1:
            return parts[0]
        return None

    def print_validation_report(
        self, report: Dict[str, Any], console: Console = None
    ) -> None:
        """Print a human-readable validation report using Rich."""
        if console is None:
            console = Console()

        if "error" in report:
            console.print(f"[red]Error: {report['error']}[/red]")
            return

        # Create summary panels
        total_councils = report["total_councils"]
        valid_scrapers = report["valid_scrapers"]
        scrapers_with_warnings = report["scrapers_with_warnings"]
        scrapers_with_errors = report["scrapers_with_errors"]

        summary_panels = [
            Panel(str(total_councils), title="Total Councils", style="blue"),
            Panel(str(valid_scrapers), title="Valid Scrapers", style="green"),
            Panel(str(scrapers_with_warnings), title="With Warnings", style="yellow"),
            Panel(str(scrapers_with_errors), title="With Errors", style="red"),
        ]

        console.print(
            Panel(
                Columns(summary_panels), title="Scraper Validation Report", style="bold"
            )
        )

        # Create CMS type distribution table
        cms_table = Table(title="CMS Type Distribution")
        cms_table.add_column("CMS Type", style="cyan")
        cms_table.add_column("Count", style="green", justify="right")
        cms_table.add_column("Percentage", style="yellow", justify="right")

        for cms_type, count in sorted(report["summary"]["cms_types"].items()):
            percentage = f"{(count / total_councils * 100):.1f}%"
            cms_table.add_row(cms_type, str(count), percentage)

        console.print(cms_table)

        # Create common issues table
        if report["summary"]["common_issues"]:
            issues_table = Table(title="Most Common Issues")
            issues_table.add_column("Issue", style="cyan")
            issues_table.add_column("Count", style="red", justify="right")

            sorted_issues = sorted(
                report["summary"]["common_issues"].items(),
                key=lambda x: x[1],
                reverse=True,
            )
            for issue, count in sorted_issues[:10]:  # Top 10
                issues_table.add_row(issue, str(count))

            console.print(issues_table)

        # Show detailed issues for councils with problems
        councils_with_issues = [
            c for c in report["councils"] if c["errors"] or c["warnings"]
        ]

        if councils_with_issues:
            console.print(
                f"\n[bold]Councils with Issues ({len(councils_with_issues)}):[/bold]"
            )

            for council in councils_with_issues[:20]:  # Limit output
                council_issues = []

                for error in council.get("errors", []):
                    council_issues.append(f"[red]✗ {error}[/red]")

                for warning in council.get("warnings", []):
                    council_issues.append(f"[yellow]⚠ {warning}[/yellow]")

                for suggestion in council.get("suggestions", []):
                    council_issues.append(f"[blue]💡 {suggestion}[/blue]")

                if council_issues:
                    console.print(
                        Panel(
                            "\n".join(council_issues),
                            title=council["council_id"],
                            style="dim",
                        )
                    )


def issue_scraper_warnings(council_id: str, service_name: str = "councillors") -> None:
    """
    Issue Python warnings for scraper consistency issues.
    This can be called during scraper execution to alert about problems.
    """
    validator = ScraperValidator()
    report = validator.validate_council_scraper(council_id)

    for warning in report.get("warnings", []):
        warnings.warn(
            f"Scraper validation warning for {council_id} ({service_name}): {warning}",
            UserWarning,
            stacklevel=2,
        )

    for error in report.get("errors", []):
        warnings.warn(
            f"Scraper validation error for {council_id} ({service_name}): {error}",
            UserWarning,
            stacklevel=2,
        )
