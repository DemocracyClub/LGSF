import datetime
import json
import sys
import traceback

from rich.console import Console

from lgsf.conf import settings
from lgsf.councillors.commands import Command
from lgsf.path_utils import load_scraper


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
    console = Console(file=sys.stdout, record=True)
    run_log = settings.RUN_LOGGER(start=datetime.datetime.utcnow())

    try:
        # For Step Functions, the council data comes directly in the event
        # rather than being wrapped in SQS Records
        if "council" in event:
            message = event
        else:
            # Fallback for different event formats
            console.log("Unexpected event format, trying to extract council data")
            console.log(f"Event: {json.dumps(event, indent=2)}")
            return {
                "statusCode": 400,
                "error": "Invalid event format - missing council data",
            }

        council = message["council"]
        command_name = message["scraper_type"]

        console.log(f"Fetching Scraper for: {council}")
        scraper_cls = load_scraper(council, command_name)

        if not scraper_cls:
            console.log(f"No scraper found for council: {council}")
            return {
                "statusCode": 404,
                "error": f"No scraper found for council: {council}",
                "council": council,
            }

        console.log(f"Begin attempting to scrape: {council}")
        options = {"council": council, "verbose": True, "aws_lambda": True}
        scraper = scraper_cls(options, console)

        try:
            if not scraper.disabled:
                scraper.run(run_log)
                console.log(f"Successfully completed scraping for: {council}")
                return {"statusCode": 200, "council": council, "status": "completed"}
            else:
                console.log(f"Scraper for {council} is disabled")
                return {"statusCode": 200, "council": council, "status": "disabled"}
        except Exception as e:
            scraper.console.log(e)
            run_log.error = traceback.format_exc()
            # This probably means finalize_storage hasn't been called.
            # Let's do that ourselves then
            scraper.finalize_storage(run_log)

            console.log(f"Error scraping {council}: {e}")
            return {
                "statusCode": 500,
                "council": council,
                "error": str(e),
                "status": "failed",
            }

    except Exception as e:
        console.log(f"Unexpected error in scraper_worker_handler: {e}")
        console.log(traceback.format_exc())
        return {"statusCode": 500, "error": str(e), "status": "failed"}


def post_processing_handler(event, context):
    """
    Post-processing handler that runs after all scrapers have completed.
    Since we use JsonPath.DISCARD to avoid payload size limits, we don't have
    individual scraper results here. This is a stub for post-processing logic.
    """
    console = Console(file=sys.stdout, record=True)
    console.log("Starting post-processing after all scrapers completed")

    try:
        # Note: We don't have individual scraper results due to JsonPath.DISCARD
        # This prevents Step Functions payload size limit issues
        console.log("Post-processing running after all scrapers have completed")
        console.log(
            "Individual scraper results are not available due to payload size optimization"
        )

        # TODO: Add any post-processing logic here, such as:
        # - Querying CloudWatch logs for scraper success/failure counts
        # - Generating summary reports from stored data
        # - Sending notifications about completion
        # - Cleaning up temporary files
        # - Triggering downstream processes
        # - Checking GitHub for updated data files

        # Example: You could query CloudWatch Logs to get scraper statistics
        # import boto3
        # logs_client = boto3.client('logs')
        # # Query logs for scraper completion/failure patterns

        console.log("Post-processing completed successfully")
        return {
            "statusCode": 200,
            "message": "Post-processing completed - all scrapers have finished execution",
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }

    except Exception as e:
        console.log(f"Error during post-processing: {e}")
        console.log(traceback.format_exc())
        return {
            "statusCode": 500,
            "error": str(e),
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
