import datetime
import json
import os
import sys
import traceback

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
                        "run_log": run_log.as_dict,
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
                    "run_log": run_log.as_dict,
                }
            )

            if storage_result:
                result["storage_result"] = storage_result

            return result

    except Exception as e:
        console.log(f"Unexpected error in scraper_worker_handler: {e}")
        console.log(traceback.format_exc())
        # Raise a structured exception so Step Functions registers it as a failure
        raise ScraperException(
            f"Scraper failed for {event.get('council', 'unknown')} ({event.get('scraper_type', 'unknown')}): {str(e)}",
            council=event.get("council", "unknown"),
            scraper_type=event.get("scraper_type", "unknown"),
            error_details={
                "statusCode": 500,
                "original_error": str(e),
                "traceback": traceback.format_exc(),
            },
        )


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
