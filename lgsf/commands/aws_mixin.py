"""
Mixin for adding AWS Step Function invocation capability to commands.

This module provides a mixin class that can be added to command classes
to enable AWS Step Function invocation via the --aws flag.

The Step Function ARN is automatically discovered based on the environment
and CDK naming convention, so no manual configuration is required.
"""

import os
from lgsf.aws_lambda.step_function_invoker import StepFunctionInvoker


class AWSInvokableMixin:
    """
    Mixin to add AWS Step Function invocation capability to command classes.

    Commands that inherit from this mixin will gain:
    - An --aws flag for triggering AWS execution
    - A --wait flag to block and show results
    - Automatic handling of AWS invocation vs local execution
    - Automatic Step Function ARN discovery
    - Console output of AWS execution status and reports

    Usage:
        class Command(AWSInvokableMixin, PerCouncilCommandBase):
            command_name = "councillors"
    """

    def add_aws_argument(self, parser):
        """
        Add AWS-related flags to the argument parser.

        Args:
            parser: argparse.ArgumentParser instance
        """
        parser.add_argument(
            "--aws",
            action="store_true",
            help="Run this command on AWS using Step Functions instead of locally",
        )
        parser.add_argument(
            "--wait",
            action="store_true",
            help="Wait for AWS execution to complete and show results (requires --aws)",
        )

    def handle_aws_invocation(self, options):
        """
        Handle AWS Step Function invocation.

        This method is called when --aws flag is present. It invokes the
        Step Function with the appropriate parameters based on the command options.

        The Step Function ARN is automatically discovered based on DC_ENVIRONMENT
        and the CDK naming convention.

        Args:
            options: Parsed command options dict

        Returns:
            None (prints output to console)
        """
        # Create invoker - ARN will be auto-discovered
        invoker = StepFunctionInvoker()

        # Determine what to invoke based on options
        council_ids = options.get("council")
        all_councils = options.get("all_councils", False)
        wait_for_completion = options.get("wait", False)

        try:
            self.console.print(
                f"[bold blue]Invoking AWS Step Function for {self.command_name} scraper...[/bold blue]"
            )

            if council_ids:
                self.console.print(f"[blue]Councils:[/blue] {council_ids}")
            elif all_councils:
                self.console.print("[blue]Running:[/blue] All councils")

            # Invoke the Step Function
            result = invoker.invoke(
                council_ids=council_ids,
                all_councils=all_councils,
            )

            # Display results
            self.console.print(
                "\n[bold green]✓ Step Function execution started successfully![/bold green]\n"
            )

            # Show discovered Step Function
            state_machine_name = result["state_machine_arn"].split(":")[-1]
            self.console.print(f"[dim]Step Function:[/dim] {state_machine_name}")

            self.console.print(
                f"[cyan]Execution Name:[/cyan] {result['execution_name']}"
            )
            self.console.print(f"[cyan]Execution ARN:[/cyan] {result['execution_arn']}")
            self.console.print(f"[cyan]Start Time:[/cyan] {result['start_date']}")

            # If --wait flag is set, wait for completion and show results
            if wait_for_completion:
                self._wait_and_show_results(invoker, result["execution_arn"])
            else:
                # Show how to check status
                self.console.print(
                    f"\n[yellow]View execution in AWS Console:[/yellow]\n"
                    f"https://console.aws.amazon.com/states/home#/executions/details/{result['execution_arn']}"
                )

        except ImportError as e:
            self.console.print(
                f"[red]Error:[/red] {e}\n"
                "boto3 is required for AWS invocation. Install with: pip install boto3"
            )
        except ValueError as e:
            # ValueError is raised when ARN cannot be discovered
            self.console.print(f"[red]Error:[/red] {e}")
        except Exception as e:
            self.console.print(f"[red]Error invoking Step Function:[/red] {e}")
            if options.get("verbose"):
                import traceback

                self.console.print(traceback.format_exc())

    def _wait_and_show_results(self, invoker, execution_arn):
        """
        Wait for execution to complete and display results.

        Args:
            invoker: StepFunctionInvoker instance
            execution_arn: ARN of the execution to wait for
        """
        from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

        self.console.print("\n[yellow]Waiting for execution to complete...[/yellow]")

        # Show spinner while waiting
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task("Running scrapers on AWS...", total=None)

            final_status = None
            for status in invoker.wait_for_execution(execution_arn):
                final_status = status

                # Update progress description based on status
                if status["status"] == "RUNNING":
                    progress.update(task, description="Running scrapers on AWS...")
                elif status["status"] == "SUCCEEDED":
                    progress.update(
                        task, description="[green]Execution completed successfully!"
                    )
                    break
                elif status["status"] in ["FAILED", "TIMED_OUT", "ABORTED"]:
                    progress.update(
                        task, description=f"[red]Execution {status['status'].lower()}"
                    )
                    break

        # Show final status
        self.console.print(f"\n[bold]Final Status:[/bold] {final_status['status']}")

        if final_status["status"] == "SUCCEEDED":
            self.console.print("[green]✓ Execution completed successfully![/green]")

            # Try to fetch and display the report
            self._display_execution_report(invoker, execution_arn, final_status)
        else:
            self.console.print(
                f"[red]✗ Execution {final_status['status'].lower()}[/red]"
            )
            if final_status.get("error"):
                self.console.print(f"[red]Error:[/red] {final_status['error']}")
            if final_status.get("cause"):
                self.console.print(f"[red]Cause:[/red] {final_status['cause']}")

    def _display_execution_report(self, invoker, execution_arn, final_status):
        """
        Fetch and display the execution report from S3.

        Args:
            invoker: StepFunctionInvoker instance
            execution_arn: ARN of the execution
            final_status: Final execution status dict
        """
        from rich.table import Table

        # Get results directly from execution output
        output = final_status.get("output", {})

        if not output:
            self.console.print("\n[yellow]Note:[/yellow] No output found in execution")
            self.console.print("[dim]Check CloudWatch Logs for execution details[/dim]")
            return

        # Display summary from aggregation step
        summary = output.get("summary", {})

        self.console.print("\n[bold]Execution Summary[/bold]")
        self.console.print(f"Total scrapers run: {summary.get('total_ran', 0)}")
        self.console.print(
            f"[green]Succeeded: {summary.get('total_succeeded', 0)}[/green]"
        )
        self.console.print(f"[red]Failed: {summary.get('total_failed', 0)}[/red]")
        self.console.print(
            f"[yellow]Rate limited: {summary.get('total_rate_limited', 0)}[/yellow]"
        )
        self.console.print(f"[dim]Disabled: {summary.get('total_disabled', 0)}[/dim]")
        self.console.print(f"Success rate: {summary.get('success_rate', 'N/A')}")

        # Display failed scrapers if any
        failed_scrapers = output.get("failed_scrapers", [])
        if failed_scrapers:
            self.console.print("\n[bold red]Failed Scrapers:[/bold red]")

            table = Table(show_header=True, header_style="bold")
            table.add_column("Council", style="cyan")
            table.add_column("Type", style="magenta")
            table.add_column("Error", style="red")

            for failed in failed_scrapers[:10]:  # Show first 10
                table.add_row(
                    failed.get("council", "Unknown"),
                    failed.get("scraper_type", "Unknown"),
                    failed.get("error", "Unknown error")[:80],  # Truncate long errors
                )

            self.console.print(table)

            if len(failed_scrapers) > 10:
                self.console.print(
                    f"[dim]... and {len(failed_scrapers) - 10} more failures[/dim]"
                )

        # Display rate limited scrapers if any
        rate_limited = output.get("rate_limited_scrapers", [])
        if rate_limited:
            self.console.print(
                f"\n[yellow]Rate Limited:[/yellow] {len(rate_limited)} scrapers hit GitHub rate limits"
            )

        # Show S3 report URL if available
        report_url = output.get("report_s3_url")
        if report_url:
            self.console.print(f"\n[dim]Full report:[/dim] {report_url}")

    def _fetch_report_with_retries(
        self,
        invoker,
        s3_bucket,
        s3_key_or_execution_id,
        use_exact_key=False,
        max_retries=12,
        retry_interval=5,
    ):
        """
        Fetch the S3 report with retries, waiting for it to be available.

        Args:
            invoker: StepFunctionInvoker instance
            s3_bucket: S3 bucket name
            s3_key_or_execution_id: Either the exact S3 key or execution ID
            use_exact_key: If True, use s3_key_or_execution_id as exact key; if False, search by execution ID
            max_retries: Maximum number of retry attempts (default: 12 = 1 minute)
            retry_interval: Seconds to wait between retries (default: 5)

        Returns:
            dict: Report data if found, None otherwise
        """
        import time
        from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task(
                "Waiting for report to be available in S3...", total=None
            )

            for attempt in range(max_retries):
                if use_exact_key:
                    # Use exact S3 key from execution output
                    report = invoker.get_s3_report_by_key(
                        s3_bucket, s3_key_or_execution_id
                    )
                else:
                    # Search by execution ID (old method)
                    report = invoker.get_s3_report(s3_bucket, s3_key_or_execution_id)

                if report:
                    progress.update(task, description="[green]Report found!")
                    return report

                # Wait before next retry (except on last attempt)
                if attempt < max_retries - 1:
                    time.sleep(retry_interval)

        # Report not found after all retries
        return None

    def _get_s3_bucket_from_ssm(self):
        """
        Get the S3 reports bucket name from SSM Parameter Store.

        Returns:
            str: S3 bucket name, or None if not found
        """
        try:
            import boto3

            # Get environment
            environment = os.environ.get("DC_ENVIRONMENT", "production")

            # Try to fetch from SSM
            ssm = boto3.client("ssm")
            parameter_name = f"/lgsf/{environment}/s3/reports_bucket"

            response = ssm.get_parameter(Name=parameter_name)
            return response["Parameter"]["Value"]

        except Exception:
            # SSM lookup failed, return None
            return None

    def should_invoke_aws(self, options):
        """
        Check if AWS invocation should be used.

        Args:
            options: Parsed command options dict

        Returns:
            bool: True if --aws flag is present
        """
        return options.get("aws", False)
