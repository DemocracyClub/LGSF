import json
import sys
import datetime

import boto3
from rich.console import Console

from lgsf.conf import settings
from lgsf.councillors.commands import Command
from lgsf.path_utils import load_scraper


def scraper_worker_handler(event, context):
    console = Console(file=sys.stdout, record=True)
    run_log = settings.RUN_LOGGER(start=datetime.datetime.utcnow())

    message = json.loads(event["Records"][0]["body"])

    council = message["council"]
    command_name = message["scraper_type"]
    console.log(f"Fetching Scraper for: {council}")
    scraper_cls = load_scraper(council, command_name)

    console.log(f"Begin attempting to scrape: {council}")
    options = {"council": council, "verbose": True, "aws_lambda": True}
    scraper = scraper_cls(options, console)
    try:
        if not scraper.disabled:
            scraper.run(run_log)
        else:
            console.log(f"Scraper for {council} is disabled")
    except Exception as e:
        scraper.console.log(e)
        run_log.error = e
        # This probably means aws_tidy_up hasn't been called.
        # Let's do that ourselves then
        scraper.aws_tidy_up(run_log)

    console.log(f"Finished running scraper for: {council}")


def queue_builder_handler(event, context):
    councillors_command = Command(argv=["", "--all-councils"], stdout=sys.stdout)
    councillors_command.options = {"all_councils": True, "exclude_missing": True}
    councils = councillors_command.councils_to_run

    sqs = boto3.resource("sqs")

    queue = sqs.get_queue_by_name(QueueName="ScraperQueue")

    for council in councils:
        message = {
            "scraper_type": "councillors",
            "council": council,
        }  # TODO Define this somewhere else so scraper_worker_handler can share it.
        queue.send_message(MessageBody=json.dumps(message))
        print(message)
