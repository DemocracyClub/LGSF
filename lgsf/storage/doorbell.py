"""
Tell subscribers that a council's repository has moved.

After each push to a council's ``main`` in LGSF-Data, one message goes to
an SNS topic of LGSF's own:

    {"repository": "STO", "after": "<sha main now points at>",
     "scraper_type": "Councillors", "run_id": "..."}

It is a doorbell, not a delivery: it carries no data, and a subscriber
such as reps-wiki fetches the repository itself and imports every commit
it hasn't seen. So a lost message costs time and a repeated or late one
costs nothing, and ringing must never fail the run that pushed: the
commits are already on ``main``, and the next push's message makes up for
a lost one.

Configured with DATA_COMMITS_TOPIC_ARN. Without it, nothing is sent, so a
run on a laptop stays quiet.
"""

from __future__ import annotations

import json
import logging
import os
import threading

logger = logging.getLogger(__name__)

TOPIC_ARN_VARIABLE = "DATA_COMMITS_TOPIC_ARN"

_clients: dict[str, object] = {}
_client_lock = threading.Lock()


def get_client(region: str):
    """
    One SNS client per region for the process; see lgsf.storage.s3.get_client.
    The region is the topic's, from its ARN, so a laptop whose AWS config
    names another region still reaches it.
    """
    with _client_lock:
        if region not in _clients:
            import boto3

            _clients[region] = boto3.client("sns", region_name=region)
        return _clients[region]


def ring(
    repository: str,
    after: str | None = None,
    topic_arn: str | None = None,
    **extra: str | None,
) -> bool:
    """
    Publish that ``repository``'s main is now at ``after``.

    Extra keyword arguments (``scraper_type``, ``run_id``) go in the
    message too; ones that are None are left out. Returns whether a
    message was sent. Never raises.
    """
    topic_arn = topic_arn or os.environ.get(TOPIC_ARN_VARIABLE)
    if not topic_arn:
        return False
    message = {"repository": repository, "after": after, **extra}
    message = {key: value for key, value in message.items() if value is not None}
    try:
        region = topic_arn.split(":")[3]
        get_client(region).publish(TopicArn=topic_arn, Message=json.dumps(message))
    except Exception as e:
        logger.warning(f"Couldn't ring the doorbell for {repository}: {e}")
        return False
    logger.info(f"Rang the doorbell for {repository} at {after}")
    return True
