import json
import sys
from datetime import datetime

import boto3
from rich.console import Console

from lgsf.councillors.commands import Command
from lgsf.path_utils import load_scraper


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
