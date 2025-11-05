import datetime
import json
import os
import sys
import traceback

import boto3
from rich.console import Console

from lgsf.aws_lambda.cloudwatch import CloudWatchLogStream
from lgsf.conf import settings
from lgsf.councillors.commands import Command
from lgsf.path_utils import load_scraper
from lgsf.storage.backends.github import GitHubRateLimitError

# Disable formatting in Rich
os.environ["TERM"] = "dumb"


def save_to_s3(bucket_name: str, key: str, data: dict, console: Console) -> bool:
    """
    Save JSON data to S3.

    Args:
        bucket_name: S3 bucket name
        key: S3 object key (path)
        data: Dictionary to save as JSON
        console: Rich console for logging

    Returns:
        True if successful, False otherwise
    """
    try:
        s3_client = boto3.client("s3")
        json_data = json.dumps(data, indent=2, default=str)

        s3_client.put_object(
            Bucket=bucket_name, Key=key, Body=json_data, ContentType="application/json"
        )

        console.log(f"Successfully saved to S3: s3://{bucket_name}/{key}")
        return True
    except Exception as e:
        console.log(f"Error saving to S3: {e}")
        console.log(traceback.format_exc())
        return False


def cleanup_old_reports(
    bucket_name: str, console: Console, keep_count: int = 10
) -> None:
    """
    Clean up old run reports and associated RunLogs, keeping only the most recent ones.

    Args:
        bucket_name: S3 bucket name
        console: Rich console for logging
        keep_count: Number of recent reports to keep (default: 10)
    """
    try:
        s3_client = boto3.client("s3")

        # List all run reports
        console.log(f"Listing run reports in s3://{bucket_name}/run-reports/")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="run-reports/")

        if "Contents" not in response:
            console.log("No run reports found to clean up")
            return

        # Sort by last modified date (newest first)
        reports = sorted(
            response["Contents"], key=lambda x: x["LastModified"], reverse=True
        )

        # Skip the most recent N reports
        reports_to_delete = reports[keep_count:]

        if not reports_to_delete:
            console.log(f"Only {len(reports)} reports found, no cleanup needed")
            return

        console.log(
            f"Found {len(reports)} reports, keeping {keep_count}, deleting {len(reports_to_delete)}"
        )

        # Extract timestamps from report filenames to find associated RunLogs
        # Format: run-report-{timestamp}-{execution_id}.json
        timestamps_to_delete = set()
        for report in reports_to_delete:
            key = report["Key"]
            filename = key.split("/")[-1]  # Get filename from key
            # Extract timestamp (format: run-report-YYYYMMDD-HHMMSS-{id}.json)
            parts = filename.replace("run-report-", "").split("-")
            if len(parts) >= 2:
                timestamp = f"{parts[0]}-{parts[1]}"  # YYYYMMDD-HHMMSS
                timestamps_to_delete.add(timestamp)

        # Delete old run reports
        for report in reports_to_delete:
            try:
                s3_client.delete_object(Bucket=bucket_name, Key=report["Key"])
                console.log(f"Deleted old report: {report['Key']}")
            except Exception as e:
                console.log(f"Error deleting {report['Key']}: {e}")

        # Delete associated RunLog directories
        for timestamp in timestamps_to_delete:
            runlog_prefix = f"runlogs/{timestamp}/"

            try:
                # List all objects in this timestamp directory
                runlog_response = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=runlog_prefix
                )

                if "Contents" in runlog_response:
                    for obj in runlog_response["Contents"]:
                        s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
                        console.log(f"Deleted old RunLog: {obj['Key']}")
            except Exception as e:
                console.log(f"Error deleting RunLogs for {timestamp}: {e}")

        console.log(
            f"Cleanup complete: deleted {len(reports_to_delete)} old reports and associated RunLogs"
        )

    except Exception as e:
        console.log(f"Error during cleanup: {e}")
        console.log(traceback.format_exc())
        # Don't raise - cleanup is best-effort and shouldn't fail the execution


class ScraperException(Exception):
    """Custom exception for scraper errors with structured data."""

    def __init__(self, message, council=None, scraper_type=None, error_details=None):
        super().__init__(message)
        self.council = council
        self.scraper_type = scraper_type
        self.error_details = error_details or {}

    def __str__(self):
        base_msg = super().__str__()
        if self.council and self.scraper_type:
            return f"{base_msg} [Council: {self.council}, Type: {self.scraper_type}]"
        return base_msg


def council_enumerator_handler(event, context):
    """
    Enumerate all councils that need to be scraped and return them as a list.
    This replaces the queue_builder_handler for Step Functions orchestration.

    Input event format:
    {
        "council_ids": "ABC,XYZ,PQR" (optional - comma-separated council IDs)
        "all_councils": true/false (optional - if true, runs all councils)
    }

    If neither is provided, defaults to all councils.
    """
    console = Console(file=sys.stdout, record=True)
    console.log("Starting council enumeration for Step Functions orchestration")

    try:
        # Extract input parameters from event
        council_ids_param = event.get("council_ids", "")

        # Build the Command based on input
        if council_ids_param:
            # Run specific councils - Command already handles comma-separated values
            console.log(f"Running specific councils: {council_ids_param}")
            councillors_command = Command(
                argv=["", "--council", council_ids_param], stdout=sys.stdout
            )
            councillors_command.options = {
                "all_councils": False,
                "council": council_ids_param,
                "exclude_missing": True,
                "exclude_disabled": True,
                "tags": None,
                "verbose": True,
            }
        else:
            # Run all councils (default behavior)
            console.log("Running all councils (excluding disabled)")
            councillors_command = Command(
                argv=["", "--all-councils"], stdout=sys.stdout
            )
            councillors_command.options = {
                "all_councils": True,
                "exclude_missing": True,
                "exclude_disabled": True,
                "council": None,
                "tags": None,
                "verbose": True,
            }

        # Get councils from the command - it handles all the parsing logic
        councils = councillors_command.councils_to_run

        # Filter to only current councils (unless specific councils were requested)
        if not council_ids_param:
            # For automated runs, only scrape current councils
            console.log("Filtering to current councils only for automated run")
            councils = [c for c in councils if c.current]
            console.log(f"Filtered to {len(councils)} current councils")

        # Convert councils to a format suitable for Step Functions Map state
        council_list = []
        for council in councils:
            council_data = {
                "scraper_type": "councillors",
                "council": council.council_id,
            }
            council_list.append(council_data)

        console.log(f"Enumerated {len(council_list)} councils for processing")

        # Return in the format expected by Step Functions Map state
        return {
            "statusCode": 200,
            "councils": council_list,
            "total_councils": len(council_list),
            "input_params": {
                "council_ids": council_ids_param if council_ids_param else "all",
                "all_councils": not bool(council_ids_param),
            },
        }

    except Exception as e:
        console.log(f"Error during council enumeration: {e}")
        console.log(traceback.format_exc())
        return {"statusCode": 500, "error": str(e), "councils": []}


def scraper_worker_handler(event, context):
    """
    Process individual scraper tasks. This is adapted from the original
    scraper_worker_handler to work with Step Functions instead of SQS.
    """
    cw_stream = CloudWatchLogStream(
        log_group=event["scraper_type"],
        log_stream=event["council"],
        flush_interval=0.2,  # tune if you log very heavily
        tee_stdout=True,
    )
    console = Console(file=cw_stream, force_terminal=True, record=True, soft_wrap=False)
    run_log = settings.RUN_LOGGER(start=datetime.datetime.now(datetime.UTC))

    # Get S3 bucket name from environment variable
    s3_bucket = os.environ.get("S3_REPORTS_BUCKET")

    try:
        # For Step Functions, the council data comes directly in the event
        # rather than being wrapped in SQS Records
        if "council" in event:
            message = event
        else:
            # Fallback for different event formats
            console.log("Unexpected event format, trying to extract council data")
            console.log(f"Event: {json.dumps(event, indent=2)}")
            raise ScraperException(
                "Invalid event format - missing council data",
                council=event.get("council", "unknown"),
                scraper_type=event.get("scraper_type", "unknown"),
                error_details={"statusCode": 400, "event": event},
            )

        council = message["council"]
        command_name = message["scraper_type"]

        console.log(f"Fetching Scraper for: {council}")
        scraper_cls = load_scraper(council, command_name)

        if not scraper_cls:
            console.log(f"No scraper found for council: {council}")
            raise ScraperException(
                f"No scraper found for council: {council}",
                council=council,
                scraper_type=command_name,
                error_details={"statusCode": 404},
            )

        console.log(f"Begin attempting to scrape: {council}")
        options = {"council": council, "verbose": True, "aws_lambda": True}
        scraper = scraper_cls(options, console)

        # Base result structure
        result = {
            "council": council,
            "scraper_type": command_name,
            "start_time": run_log.start.isoformat(),
        }

        try:
            if not scraper.disabled:
                scraper.run(run_log)
                console.log(f"Successfully completed scraping for: {council}")

                # For 200 responses, only return status code - no run log or other text
                result.update(
                    {
                        "statusCode": 200,
                        "status": "completed",
                    }
                )

                return result
            else:
                console.log(f"Scraper for {council} is disabled")
                result.update(
                    {
                        "statusCode": 200,
                        "status": "disabled",
                    }
                )
                return result
        except GitHubRateLimitError as e:
            # GitHub rate limit error - return 429 status with minimal context
            console.log(f"GitHub rate limit for {council}: {e}")
            run_log.error = str(e)
            run_log.finish()

            # Save RunLog to S3 for rate limited scrapers
            runlog_s3_key = None
            if s3_bucket:
                timestamp = run_log.start.strftime("%Y%m%d-%H%M%S")
                runlog_s3_key = f"runlogs/{timestamp}/{council}-{command_name}.json"

                runlog_data = {
                    "council": council,
                    "scraper_type": command_name,
                    "status": "rate_limited",
                    "start_time": run_log.start.isoformat(),
                    "end_time": run_log.end.isoformat() if run_log.end else None,
                    "duration_seconds": run_log.duration.total_seconds()
                    if run_log.duration
                    else 0,
                    "error": run_log.error,
                    "error_type": "GitHubRateLimitError",
                }

                save_to_s3(s3_bucket, runlog_s3_key, runlog_data, console)

            result.update(
                {
                    "statusCode": 429,
                    "error": "GHRateLimited",
                    "status": "failed",
                    "runlog_s3_key": runlog_s3_key,
                }
            )
            return result
        except Exception as e:
            # For scraper exceptions, store 500 and a very short exception headline
            # Extract just the exception type and message, no traceback
            scraper.console.log(e)
            run_log.error = traceback.format_exc()
            run_log.finish()

            # This probably means finalize_storage hasn't been called.
            # Let's do that ourselves then
            try:
                scraper.finalize_storage(run_log)
            except Exception:
                pass  # Ignore finalization errors

            console.log(f"Error scraping {council}: {e}")

            # Get short error message - just exception type and first line
            error_message = str(e).split("\n")[0][:150]
            exception_type = type(e).__name__

            # Save RunLog to S3 for failed scrapers
            runlog_s3_key = None
            if s3_bucket:
                timestamp = run_log.start.strftime("%Y%m%d-%H%M%S")
                runlog_s3_key = f"runlogs/{timestamp}/{council}-{command_name}.json"

                runlog_data = {
                    "council": council,
                    "scraper_type": command_name,
                    "status": "failed",
                    "start_time": run_log.start.isoformat(),
                    "end_time": run_log.end.isoformat() if run_log.end else None,
                    "duration_seconds": run_log.duration.total_seconds()
                    if run_log.duration
                    else 0,
                    "error": run_log.error,
                    "error_type": exception_type,
                    "error_message": error_message,
                }

                save_to_s3(s3_bucket, runlog_s3_key, runlog_data, console)

            result.update(
                {
                    "statusCode": 500,
                    "error": f"{exception_type}: {error_message}",
                    "status": "failed",
                    "runlog_s3_key": runlog_s3_key,
                }
            )

            return result

    except GitHubRateLimitError as e:
        console.log(f"GitHub rate limit error in scraper_worker_handler: {e}")

        council = event.get("council", "unknown")
        scraper_type = event.get("scraper_type", "unknown")

        # Save minimal RunLog to S3
        runlog_s3_key = None
        if s3_bucket:
            timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d-%H%M%S")
            runlog_s3_key = f"runlogs/{timestamp}/{council}-{scraper_type}.json"

            runlog_data = {
                "council": council,
                "scraper_type": scraper_type,
                "status": "rate_limited",
                "start_time": datetime.datetime.now(datetime.UTC).isoformat(),
                "error": str(e),
                "error_type": "GitHubRateLimitError",
            }

            save_to_s3(s3_bucket, runlog_s3_key, runlog_data, console)

        # Return rate limit error with minimal context
        return {
            "council": council,
            "scraper_type": scraper_type,
            "statusCode": 429,
            "status": "failed",
            "error": "GHRateLimited",
            "runlog_s3_key": runlog_s3_key,
        }
    except Exception as e:
        console.log(f"Unexpected error in scraper_worker_handler: {e}")
        console.log(traceback.format_exc())

        council = event.get("council", "unknown")
        scraper_type = event.get("scraper_type", "unknown")

        # Get short error message - just exception type and first line
        error_message = str(e).split("\n")[0][:150]
        exception_type = type(e).__name__

        # Save minimal RunLog to S3
        runlog_s3_key = None
        if s3_bucket:
            timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d-%H%M%S")
            runlog_s3_key = f"runlogs/{timestamp}/{council}-{scraper_type}.json"

            runlog_data = {
                "council": council,
                "scraper_type": scraper_type,
                "status": "failed",
                "start_time": datetime.datetime.now(datetime.UTC).isoformat(),
                "error": traceback.format_exc(),
                "error_type": exception_type,
                "error_message": error_message,
            }

            save_to_s3(s3_bucket, runlog_s3_key, runlog_data, console)

        # Return error information rather than raising to prevent map failure
        return {
            "council": council,
            "scraper_type": scraper_type,
            "statusCode": 500,
            "status": "failed",
            "error": f"{exception_type}: {error_message}",
            "runlog_s3_key": runlog_s3_key,
        }


def post_processing_handler(event, context):
    """
    Post-processing handler that runs after all scrapers have completed.
    Now processes individual scraper results since we store them in job data.
    """
    console = Console(file=sys.stdout, record=True)
    console.log("Starting post-processing after all scrapers completed")

    try:
        # Extract scraper results from the event
        scraper_results = event.get("scraper_results", [])
        console.log(f"Processing results from {len(scraper_results)} scrapers")

        # Analyze results
        successful_scrapers = []
        failed_scrapers = []
        disabled_scrapers = []
        total_files_committed = 0

        for result in scraper_results:
            council = result.get("council", "unknown")
            scraper_type = result.get("scraper_type", "unknown")
            status = result.get("status", "unknown")

            if status == "completed":
                successful_scrapers.append(f"{council} ({scraper_type})")
                if "files_committed" in result:
                    total_files_committed += result["files_committed"]
            elif status == "failed":
                failed_scrapers.append(
                    f"{council} ({scraper_type}): {result.get('error', 'Unknown error')}"
                )
            elif status == "disabled":
                disabled_scrapers.append(f"{council} ({scraper_type})")

        # Log summary
        console.log(
            f"Summary: {len(successful_scrapers)} successful, {len(failed_scrapers)} failed, {len(disabled_scrapers)} disabled"
        )
        console.log(f"Total files committed: {total_files_committed}")

        if failed_scrapers:
            console.log("Failed scrapers:")
            for failure in failed_scrapers:
                console.log(f"  - {failure}")

        # TODO: Add additional post-processing logic here, such as:
        # - Sending notifications about completion/failures
        # - Generating summary reports from stored data
        # - Cleaning up temporary files
        # - Triggering downstream processes

        console.log("Post-processing completed successfully")
        return {
            "statusCode": 200,
            "message": "Post-processing completed - all scrapers have finished execution",
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "summary": {
                "total_scrapers": len(scraper_results),
                "successful": len(successful_scrapers),
                "failed": len(failed_scrapers),
                "disabled": len(disabled_scrapers),
                "total_files_committed": total_files_committed,
            },
            "failed_scrapers": failed_scrapers if failed_scrapers else None,
        }

    except Exception as e:
        console.log(f"Error during post-processing: {e}")
        console.log(traceback.format_exc())
        return {
            "statusCode": 500,
            "error": str(e),
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }


def aggregation_handler(event, context):
    """
    Aggregate results from the distributed map execution.
    Collects statistics about successes, failures, and disabled scrapers.
    Saves a comprehensive run report to S3.
    """
    console = Console(file=sys.stdout, record=True)
    console.log("Starting result aggregation")

    # Get S3 bucket name and execution ID from environment
    s3_bucket = os.environ.get("S3_REPORTS_BUCKET")
    execution_id = context.aws_request_id if context else "unknown"

    try:
        # Extract scraper results from the distributed map output
        scraper_results = event.get("scraper_results", [])

        # Handle both direct results and nested Payload structure from Lambda invocations
        processed_results = []
        for result in scraper_results:
            # If the result has a Payload key, extract it (Lambda invocation result)
            if isinstance(result, dict) and "Payload" in result:
                processed_results.append(result["Payload"])
            else:
                processed_results.append(result)

        console.log(f"Processing {len(processed_results)} scraper results")

        # Initialize counters and lists
        total_ran = 0
        total_succeeded = 0
        total_failed = 0
        total_disabled = 0
        total_rate_limited = 0

        successful_scrapers = []
        failed_scrapers = []
        disabled_scrapers = []
        rate_limited_scrapers = []

        # Process each result
        for result in processed_results:
            if not isinstance(result, dict):
                console.log(f"Skipping invalid result: {result}")
                continue

            council = result.get("council", "unknown")
            scraper_type = result.get("scraper_type", "unknown")
            status = result.get("status", "unknown")
            status_code = result.get("statusCode", 200)

            total_ran += 1

            if status == "completed":
                total_succeeded += 1
                successful_scrapers.append(
                    {
                        "council": council,
                        "scraper_type": scraper_type,
                    }
                )
            elif status_code == 429:
                # GitHub rate limited
                total_rate_limited += 1
                rate_limited_scrapers.append(
                    {
                        "council": council,
                        "scraper_type": scraper_type,
                        "error": result.get("error", "GHRateLimited"),
                    }
                )
            elif status == "failed":
                total_failed += 1
                failed_scrapers.append(
                    {
                        "council": council,
                        "scraper_type": scraper_type,
                        "error": result.get("error", "Unknown error"),
                    }
                )
            elif status == "disabled":
                total_disabled += 1
                disabled_scrapers.append(
                    {
                        "council": council,
                        "scraper_type": scraper_type,
                    }
                )

        # Create summary
        summary = {
            "total_ran": total_ran,
            "total_succeeded": total_succeeded,
            "total_failed": total_failed,
            "total_rate_limited": total_rate_limited,
            "total_disabled": total_disabled,
            "success_rate": f"{(total_succeeded / total_ran * 100) if total_ran > 0 else 0:.2f}%",
        }

        console.log(f"Aggregation summary: {summary}")

        # Build the complete report data structure
        timestamp = datetime.datetime.now(datetime.UTC)
        report_data = {
            "execution_id": execution_id,
            "timestamp": timestamp.isoformat(),
            "summary": summary,
            "scrapers": [],
        }

        # Build detailed scraper list with all information
        for result in processed_results:
            if not isinstance(result, dict):
                continue

            scraper_entry = {
                "council": result.get("council", "unknown"),
                "scraper_type": result.get("scraper_type", "unknown"),
                "status": result.get("status", "unknown"),
                "status_code": result.get("statusCode", 200),
                "start_time": result.get("start_time"),
            }

            # Add error information if present
            if result.get("error"):
                scraper_entry["error"] = result.get("error")

            # Add link to detailed RunLog if available
            if result.get("runlog_s3_key"):
                scraper_entry["runlog_s3_key"] = result.get("runlog_s3_key")
                scraper_entry["runlog_s3_url"] = (
                    f"s3://{s3_bucket}/{result.get('runlog_s3_key')}"
                )

            report_data["scrapers"].append(scraper_entry)

        # Save comprehensive run report to S3
        report_s3_key = None
        if s3_bucket:
            timestamp_str = timestamp.strftime("%Y%m%d-%H%M%S")
            report_s3_key = (
                f"run-reports/run-report-{timestamp_str}-{execution_id[:8]}.json"
            )

            if save_to_s3(s3_bucket, report_s3_key, report_data, console):
                console.log(f"Run report saved to S3: s3://{s3_bucket}/{report_s3_key}")

                # Clean up old reports, keeping only the last 10
                console.log("Starting cleanup of old reports...")
                cleanup_old_reports(s3_bucket, console, keep_count=10)
            else:
                console.log("Failed to save run report to S3")

        return {
            "statusCode": 200,
            "timestamp": timestamp.isoformat(),
            "summary": summary,
            "successful_scrapers": successful_scrapers,
            "failed_scrapers": failed_scrapers,
            "rate_limited_scrapers": rate_limited_scrapers,
            "disabled_scrapers": disabled_scrapers,
            "report_s3_key": report_s3_key,
            "report_s3_url": f"s3://{s3_bucket}/{report_s3_key}"
            if report_s3_key and s3_bucket
            else None,
        }

    except Exception as e:
        console.log(f"Error during aggregation: {e}")
        console.log(traceback.format_exc())
        return {
            "statusCode": 500,
            "error": str(e),
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        }


def send_report_notification_handler(event, context):
    """
    Send a summary notification using SNS with execution statistics.
    """
    console = Console(file=sys.stdout, record=True)
    console.log("Starting notification report generation")

    try:
        # Extract aggregation results
        summary = event.get("summary", {})
        successful_scrapers = event.get("successful_scrapers", [])
        failed_scrapers = event.get("failed_scrapers", [])
        rate_limited_scrapers = event.get("rate_limited_scrapers", [])
        disabled_scrapers = event.get("disabled_scrapers", [])
        timestamp = event.get(
            "timestamp", datetime.datetime.now(datetime.UTC).isoformat()
        )

        # Build notification subject and message
        total_failed = summary.get("total_failed", 0)
        total_rate_limited = summary.get("total_rate_limited", 0)
        subject = f"LGSF Scraper Report: {summary.get('total_ran', 0)} councils - {total_failed} failed, {total_rate_limited} rate limited"

        message = f"""LGSF Scraper Execution Report
{"=" * 60}
Completed: {timestamp}

SUMMARY
-------
Total Ran:      {summary.get("total_ran", 0)}
Succeeded:      {summary.get("total_succeeded", 0)}
Failed:         {summary.get("total_failed", 0)}
Rate Limited:   {summary.get("total_rate_limited", 0)}
Disabled:       {summary.get("total_disabled", 0)}
Success Rate:   {summary.get("success_rate", "N/A")}
"""

        # Add rate limited scrapers section if any
        if rate_limited_scrapers:
            message += (
                f"\n\nRATE LIMITED SCRAPERS ({len(rate_limited_scrapers)} total)\n"
            )
            message += "-" * 60 + "\n"
            for rate_limited in rate_limited_scrapers[:20]:
                council = rate_limited.get("council", "unknown")
                scraper_type = rate_limited.get("scraper_type", "unknown")
                message += f"• {council} ({scraper_type})\n"

            if len(rate_limited_scrapers) > 20:
                message += (
                    f"\n... and {len(rate_limited_scrapers) - 20} more rate limited\n"
                )

        # Add failed scrapers section if any
        if failed_scrapers:
            message += f"\n\nFAILED SCRAPERS ({len(failed_scrapers)} total)\n"
            message += "-" * 60 + "\n"
            # Limit to first 20 to keep notification concise
            for failed in failed_scrapers[:20]:
                council = failed.get("council", "unknown")
                scraper_type = failed.get("scraper_type", "unknown")
                error = failed.get("error", "Unknown error")
                # Error is already truncated in the handler
                message += f"\n• {council} ({scraper_type})\n  {error}\n"

            if len(failed_scrapers) > 20:
                message += f"\n... and {len(failed_scrapers) - 20} more failures\n"

        # Add brief successful scrapers note
        if successful_scrapers:
            message += f"\n\nSUCCESSFUL: {len(successful_scrapers)} scrapers completed without errors\n"

        # Add disabled scrapers note if any
        if disabled_scrapers:
            message += f"\nDISABLED: {len(disabled_scrapers)} scrapers were skipped\n"

        message += "\n" + "=" * 60 + "\n"
        message += (
            "Automated report from LGSF scraper orchestration (AWS Step Functions)\n"
        )

        # Publish to SNS topic
        sns_client = boto3.client("sns")

        # Get topic ARN from environment variable
        topic_arn = os.environ.get("SNS_TOPIC_ARN")

        if not topic_arn:
            raise ValueError("SNS_TOPIC_ARN environment variable not set")

        console.log(f"Publishing notification to SNS topic: {topic_arn}")

        response = sns_client.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message,
        )

        console.log(
            f"Notification published successfully. MessageId: {response['MessageId']}"
        )

        return {
            "statusCode": 200,
            "message": "Notification sent successfully",
            "messageId": response["MessageId"],
            "topicArn": topic_arn,
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        }

    except Exception as e:
        console.log(f"Error sending notification report: {e}")
        console.log(traceback.format_exc())
        return {
            "statusCode": 500,
            "error": str(e),
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        }
