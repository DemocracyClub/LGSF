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

os.environ["TERM"] = "dumb"


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
    """
    console = Console(file=sys.stdout, record=True)
    console.log("Starting council enumeration for Step Functions orchestration")

    try:
        # Use the same logic as the original queue_builder_handler to get councils
        councillors_command = Command(argv=["", "--all-councils"], stdout=sys.stdout)
        councillors_command.options = {
            "all_councils": True,
            "exclude_missing": True,
            "exclude_disabled": True,
        }
        councils = councillors_command.councils_to_run

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

                # Get storage result information if available
                storage_result = None
                if hasattr(scraper, "storage_session") and scraper.storage_session:
                    # Storage session should have been finalized by scraper.run()
                    # but let's make sure we get the result info
                    if hasattr(scraper, "_last_storage_result"):
                        storage_result = scraper._last_storage_result

                result.update(
                    {
                        "statusCode": run_log.as_lambda_status_code(),
                        "status": "completed",
                        "end_time": run_log.end.isoformat() if run_log.end else None,
                        "duration": str(run_log.duration) if run_log.duration else None,
                        "run_log": json.loads(run_log.as_json),
                    }
                )

                if storage_result:
                    result["storage_result"] = storage_result
                    # Extract result paths if available
                    if (
                        isinstance(storage_result, dict)
                        and "files_committed" in storage_result
                    ):
                        result["files_committed"] = storage_result["files_committed"]

                return result
            else:
                console.log(f"Scraper for {council} is disabled")
                result.update(
                    {
                        "statusCode": run_log.as_lambda_status_code(),
                        "status": "disabled",
                        "end_time": run_log.end.isoformat() if run_log.end else None,
                    }
                )
                return result
        except Exception as e:
            scraper.console.log(e)
            run_log.error = traceback.format_exc()
            # This probably means finalize_storage hasn't been called.
            # Let's do that ourselves then
            storage_result = scraper.finalize_storage(run_log)

            console.log(f"Error scraping {council}: {e}")
            result.update(
                {
                    "statusCode": run_log.as_lambda_status_code(),
                    "error": str(e),
                    "status": "failed",
                    "end_time": run_log.end.isoformat() if run_log.end else None,
                    "duration": str(run_log.duration) if run_log.duration else None,
                    "run_log": json.loads(run_log.as_json),
                }
            )

            if storage_result:
                result["storage_result"] = storage_result

            return result

    except Exception as e:
        console.log(f"Unexpected error in scraper_worker_handler: {e}")
        console.log(traceback.format_exc())
        # Return error information rather than raising to prevent map failure
        return {
            "council": event.get("council", "unknown"),
            "scraper_type": event.get("scraper_type", "unknown"),
            "statusCode": 500,
            "status": "failed",
            "error": str(e),
            "error_details": {
                "original_error": str(e),
                "traceback": traceback.format_exc(),
            },
            "end_time": datetime.datetime.now(datetime.UTC).isoformat(),
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
    """
    console = Console(file=sys.stdout, record=True)
    console.log("Starting result aggregation")

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

        successful_scrapers = []
        failed_scrapers = []
        disabled_scrapers = []

        # Process each result
        for result in processed_results:
            if not isinstance(result, dict):
                console.log(f"Skipping invalid result: {result}")
                continue

            council = result.get("council", "unknown")
            scraper_type = result.get("scraper_type", "unknown")
            status = result.get("status", "unknown")

            total_ran += 1

            if status == "completed":
                total_succeeded += 1
                successful_scrapers.append(
                    {
                        "council": council,
                        "scraper_type": scraper_type,
                        "duration": result.get("duration"),
                    }
                )
            elif status == "failed":
                total_failed += 1
                failed_scrapers.append(
                    {
                        "council": council,
                        "scraper_type": scraper_type,
                        "error": result.get("error", "Unknown error"),
                        "error_details": result.get("error_details", {}),
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
            "total_disabled": total_disabled,
            "success_rate": f"{(total_succeeded / total_ran * 100) if total_ran > 0 else 0:.2f}%",
        }

        console.log(f"Aggregation summary: {summary}")

        return {
            "statusCode": 200,
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "summary": summary,
            "successful_scrapers": successful_scrapers,
            "failed_scrapers": failed_scrapers,
            "disabled_scrapers": disabled_scrapers,
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
        disabled_scrapers = event.get("disabled_scrapers", [])
        timestamp = event.get(
            "timestamp", datetime.datetime.now(datetime.UTC).isoformat()
        )

        # Build notification subject and message
        subject = f"LGSF Scraper Report: {summary.get('total_ran', 0)} councils - {summary.get('total_failed', 0)} failed"

        message = f"""LGSF Scraper Execution Report
{"=" * 60}
Completed: {timestamp}

SUMMARY
-------
Total Ran:    {summary.get("total_ran", 0)}
Succeeded:    {summary.get("total_succeeded", 0)}
Failed:       {summary.get("total_failed", 0)}
Disabled:     {summary.get("total_disabled", 0)}
Success Rate: {summary.get("success_rate", "N/A")}
"""

        # Add failed scrapers section if any
        if failed_scrapers:
            message += f"\n\nFAILED SCRAPERS ({len(failed_scrapers)} total)\n"
            message += "-" * 60 + "\n"
            # Limit to first 20 to keep notification concise
            for failed in failed_scrapers[:20]:
                council = failed.get("council", "unknown")
                scraper_type = failed.get("scraper_type", "unknown")
                error = failed.get("error", "Unknown error")
                # Truncate long error messages
                error_msg = error[:150] + "..." if len(error) > 150 else error
                message += f"\n• {council} ({scraper_type})\n  {error_msg}\n"

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
