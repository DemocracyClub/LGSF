"""
The document pipeline in AWS: the Lambda handlers, the queues between
them, and the "file ready" message. See docs/document-pipeline.md,
"Running in AWS".

Every message on every queue is one file, as JSON::

    {"council": "STO", "storage_key": "<sha256>.pdf"}

The trigger finds files to do and puts them on the documents queue.
Dispatch settles what it can without Docling and routes the rest to the
extract queue (for the Batch worker, lgsf.embeddings.queue_worker) or
the embed queue. The embed handler embeds. Whenever a file's status
reaches a final outcome, one message goes to the "file ready" topic.

This module imports without Docling: the Lambdas run with boto3 alone.
The clients come from get_client(), which tests replace.
"""

import datetime
import json
import logging
import os
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from lgsf.embeddings import pipeline
from lgsf.embeddings.embedders import EMBEDDERS, EmbedderBase
from lgsf.embeddings.files import File
from lgsf.embeddings.sidecars import FileStatus, Outcome, Stage, Suffix, result_key
from lgsf.embeddings.stores import S3Store
from lgsf.storage.documents.base import is_content_key
from lgsf.storage.s3 import DOCUMENTS_DIR

logger = logging.getLogger(__name__)

# The environment variables the handlers and the worker read. The bucket
# and prefix are LGSF_S3_BUCKET and LGSF_S3_PREFIX, read by S3Store.
DOCUMENTS_QUEUE_URL = "DOCUMENTS_QUEUE_URL"
EXTRACT_QUEUE_URL = "EXTRACT_QUEUE_URL"
EMBED_QUEUE_URL = "EMBED_QUEUE_URL"
FILE_READY_TOPIC_ARN = "FILE_READY_TOPIC_ARN"
DOCUMENTS_BATCH_JOB_QUEUE = "DOCUMENTS_BATCH_JOB_QUEUE"
DOCUMENTS_BATCH_JOB_DEFINITION = "DOCUMENTS_BATCH_JOB_DEFINITION"
TRIGGER_FUNCTION_NAME = "TRIGGER_FUNCTION_NAME"
EMBEDDER = "EMBEDDER"
#: An ISO UTC timestamp. The trigger queues only files written to the
#: bucket at or after it; older files come in through the backfill
#: command, which sets its own pace. Unset, the trigger queues nothing.
DOCUMENTS_SINCE = "DOCUMENTS_SINCE"

DEFAULT_EMBEDDER = "titan-v2"

#: Of the "file ready" message.
FILE_READY_SCHEMA_VERSION = 1

#: SQS takes at most ten messages in one request.
BATCH_SIZE = 10

#: Outcomes that are final without any embedding.
FINAL_WITHOUT_EMBEDDING = frozenset(
    {Outcome.RESTRICTED, Outcome.EMPTY, Outcome.NOT_A_DOCUMENT, Outcome.FAILED}
)

#: A Batch job in one of these states is already working, or about to.
BATCH_ACTIVE_STATUSES = ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING")

#: Written beside a file when it's sent to the extract queue, so that a
#: sweep or a backfill doesn't queue it again while it waits there. Not
#: one of the pipeline's result files, and nothing removes it: once the
#: status is written the file is skipped anyway.
QUEUED_SUFFIX = "queued.json"

#: How long a marker holds a file back: longer than a message can wait
#: on the extract queue through a backlog, shorter than the queue's
#: retention, so that a lost message is made up for by a later sweep.
QUEUED_FOR = datetime.timedelta(days=7)

_clients: dict[str, object] = {}
_client_lock = threading.Lock()


def get_client(service: str):
    """One boto3 client per service for the process."""
    with _client_lock:
        if service not in _clients:
            import boto3

            _clients[service] = boto3.client(service)
        return _clients[service]


def environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise OSError(f"{name} isn't set")
    return value


def store() -> S3Store:
    """The bucket LGSF_S3_BUCKET and LGSF_S3_PREFIX name."""
    return S3Store(client=get_client("s3"))


def embedder_name() -> str:
    """The embedder the Lambdas use: EMBEDDER, or titan-v2."""
    return os.environ.get(EMBEDDER) or DEFAULT_EMBEDDER


def make_embedder() -> EmbedderBase:
    return EMBEDDERS[embedder_name()]()


def utc_timestamp() -> str:
    return datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def documents_since() -> datetime.datetime | None:
    """
    The floor DOCUMENTS_SINCE sets, as an aware UTC datetime, or None if
    it isn't set. Raises ValueError for a value that isn't a timestamp.
    """
    value = os.environ.get(DOCUMENTS_SINCE)
    if not value:
        return None
    since = datetime.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if since.tzinfo is None:
        since = since.replace(tzinfo=datetime.UTC)
    return since.astimezone(datetime.UTC)


# Messages


def message(file: File) -> dict:
    return {"council": file.council, "storage_key": file.storage_key}


def file_from_message(body: str | bytes | dict) -> File:
    """
    The File a queue message is about. Raises ValueError for a body
    that isn't one.
    """
    if not isinstance(body, dict):
        body = json.loads(body)
    council = body.get("council")
    storage_key = body.get("storage_key")
    if not council or not storage_key or not is_content_key(str(storage_key)):
        raise ValueError(f"Not a file message: {body!r}")
    return File(str(council).upper(), str(storage_key))


def batches(items: Sequence, size: int = BATCH_SIZE):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def send_files(queue_url: str, files: Sequence[File], sqs=None) -> int:
    """
    Put one message a file on a queue, ten a request. Raises
    RuntimeError if SQS refused any of them. Returns how many were sent.
    """
    sqs = sqs or get_client("sqs")
    sent = 0
    for batch in batches(list(files)):
        response = sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": str(index), "MessageBody": json.dumps(message(file))}
                for index, file in enumerate(batch)
            ],
        )
        failed = response.get("Failed") or []
        if failed:
            raise RuntimeError(
                f"{len(failed)} of {len(batch)} messages not sent to {queue_url}: "
                f"{failed[0].get('Message') or failed[0].get('Code')}"
            )
        sent += len(batch)
    return sent


def queue_depth(queue_url: str, sqs=None) -> tuple[int, int]:
    """How many messages are waiting on a queue, and how many in flight."""
    sqs = sqs or get_client("sqs")
    response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )
    attributes = response.get("Attributes") or {}
    return (
        int(attributes.get("ApproximateNumberOfMessages", 0)),
        int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0)),
    )


# "File ready"


def file_ready_message(file: File, status: FileStatus) -> dict:
    return {
        "council": file.council,
        "storage_key": file.storage_key,
        "content_hash": file.content_hash,
        "outcome": str(status.outcome) if status.outcome else None,
        "status_key": result_key(file.document_key, Suffix.STATUS),
        "schema_version": FILE_READY_SCHEMA_VERSION,
    }


def is_final(status: FileStatus | None, embedder: str) -> bool:
    """
    Whether a status is one "file ready" is published for: a file left
    out or failed, or one whose chunks ``embedder`` has embedded.
    """
    if status is None:
        return False
    if status.outcome in FINAL_WITHOUT_EMBEDDING:
        return True
    return status.outcome == Outcome.OK and status.embedded(embedder)


@dataclass
class Targets:
    """
    Where the pipeline's messages go, from the environment. Each Lambda
    is given only the variables it needs, so a target is checked when
    it's used, not here.

    A queue that isn't set raises when a file is sent to it: that file
    would otherwise be lost. A topic that isn't set is skipped with a
    warning: a "file ready" message is a notice, and a subscriber can
    always read the status files.
    """

    documents_queue_url: str | None = None
    extract_queue_url: str | None = None
    embed_queue_url: str | None = None
    file_ready_topic_arn: str | None = None
    sqs: object = None
    sns: object = None

    @classmethod
    def from_environment(cls, sqs=None, sns=None) -> "Targets":
        return cls(
            documents_queue_url=os.environ.get(DOCUMENTS_QUEUE_URL),
            extract_queue_url=os.environ.get(EXTRACT_QUEUE_URL),
            embed_queue_url=os.environ.get(EMBED_QUEUE_URL),
            file_ready_topic_arn=os.environ.get(FILE_READY_TOPIC_ARN),
            sqs=sqs,
            sns=sns,
        )

    def _send(self, queue_url: str | None, variable: str, files: Sequence[File]):
        if not files:
            return 0
        if not queue_url:
            raise OSError(f"{variable} isn't set")
        return send_files(queue_url, files, self.sqs or get_client("sqs"))

    def to_documents(self, files: Sequence[File]) -> int:
        return self._send(self.documents_queue_url, DOCUMENTS_QUEUE_URL, files)

    def to_extract(self, files: Sequence[File], store=None) -> int:
        """
        Send files to the converter, and if given the store, mark each
        as queued (see mark_queued()) once they're sent.
        """
        sent = self._send(self.extract_queue_url, EXTRACT_QUEUE_URL, files)
        if store is not None:
            for file in files:
                mark_queued(store, file)
        return sent

    def to_embed(self, files: Sequence[File]) -> int:
        return self._send(self.embed_queue_url, EMBED_QUEUE_URL, files)

    def ready(self, file: File, status: FileStatus) -> bool:
        """Publish that a file's status has reached a final outcome."""
        if not self.file_ready_topic_arn:
            logger.warning(
                f"{FILE_READY_TOPIC_ARN} isn't set: not publishing that "
                f"{file.document_key} is ready"
            )
            return False
        sns = self.sns or get_client("sns")
        sns.publish(
            TopicArn=self.file_ready_topic_arn,
            Message=json.dumps(file_ready_message(file, status)),
        )
        return True


# The queued marker


def mark_queued(store, file: File):
    """
    Write ``<key>.queued.json`` beside a file sent to the extract queue.
    A sweep or a backfill skips a file whose marker is younger than
    QUEUED_FOR; the pipeline's own stages never read it.
    """
    store.write_bytes(
        result_key(file.document_key, QUEUED_SUFFIX),
        json.dumps(
            {"queued_at": utc_now().isoformat(timespec="seconds"), "queue": "extract"}
        ).encode("utf-8"),
    )


def recently_queued(entries, document_key: str, now=None) -> bool:
    """Whether a listing has a marker for the file younger than QUEUED_FOR."""
    entry = entries.get(result_key(document_key, QUEUED_SUFFIX))
    if entry is None:
        return False
    return entry.last_modified > (now or utc_now()) - QUEUED_FOR


def recently_queued_files(store, files: Sequence[File]) -> set[File]:
    """The files among ``files`` with a fresh marker, one listing a council."""
    now = utc_now()
    queued = set()
    for council in sorted({file.council for file in files}):
        entries = store.list_entries(f"{council}/{DOCUMENTS_DIR}/")
        queued.update(
            file
            for file in files
            if file.council == council
            and recently_queued(entries, file.document_key, now)
        )
    return queued


# The trigger: what to queue


def files_without_status(
    store, council: str, since: datetime.datetime | None = None
) -> list[File]:
    """
    A council's documents that have no status file and aren't waiting
    on the extract queue (no fresh marker), from one listing of its
    documents directory. With ``since``, only those written to the
    store at or after it.
    """
    prefix = f"{council}/{DOCUMENTS_DIR}/"
    entries = store.list_entries(prefix)
    now = utc_now()
    return [
        File(council, key.removeprefix(prefix))
        for key in sorted(entries)
        if is_content_key(key.removeprefix(prefix))
        and (since is None or entries[key].last_modified >= since)
        and result_key(key, Suffix.STATUS) not in entries
        and not recently_queued(entries, key, now)
    ]


def list_councils(store: S3Store) -> list[str]:
    """The council codes with anything under the prefix: its directories."""
    root = f"{store.prefix}/" if store.prefix else ""
    councils = []
    paginator = store.client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=store.bucket, Prefix=root, Delimiter="/"):
        for common in page.get("CommonPrefixes", []):
            councils.append(common["Prefix"][len(root) :].rstrip("/"))
    return sorted(councils)


def queue_council(store, council: str, targets: Targets) -> int:
    """
    Put a council's files that have no status, written since
    DOCUMENTS_SINCE, on the documents queue. Without the variable
    nothing is queued: a sweep of the whole bucket would otherwise put
    every historical file on the queue at once, which is the backfill
    command's job, at its own pace.
    """
    since = documents_since()
    if since is None:
        logger.warning(
            f"{DOCUMENTS_SINCE} isn't set: not queueing anything for {council}"
        )
        return 0
    files = files_without_status(store, council, since)
    sent = targets.to_documents(files)
    logger.info(f"{council}: {sent} files queued")
    return sent


def sweep(store: S3Store, function_name: str | None = None, lambda_client=None):
    """
    Invoke the trigger once a council, asynchronously, so that a sweep of
    every council never runs long in one invocation.
    """
    function_name = function_name or environment(TRIGGER_FUNCTION_NAME)
    lambda_client = lambda_client or get_client("lambda")
    councils = list_councils(store)
    for council in councils:
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=json.dumps({"council": council}),
        )
    return councils


def active_batch_jobs(job_queue: str, batch=None) -> list[str]:
    """The names of jobs on the queue that are running or about to."""
    batch = batch or get_client("batch")
    names = []
    for status in BATCH_ACTIVE_STATUSES:
        response = batch.list_jobs(jobQueue=job_queue, jobStatus=status)
        names += [job["jobName"] for job in response.get("jobSummaryList", [])]
    return names


def submit_batch(sqs=None, batch=None) -> dict:
    """
    Submit a Batch job to drain the extract queue, if the queue has
    messages waiting and no job is already running or about to.
    """
    extract_queue_url = environment(EXTRACT_QUEUE_URL)
    job_queue = environment(DOCUMENTS_BATCH_JOB_QUEUE)
    visible, in_flight = queue_depth(extract_queue_url, sqs)
    result = {
        "action": "submit_batch",
        "submitted": False,
        "visible": visible,
        "in_flight": in_flight,
    }
    if not visible:
        return {**result, "reason": "the extract queue is empty"}
    active = active_batch_jobs(job_queue, batch)
    if active:
        return {**result, "reason": f"{active[0]} is already on the job queue"}
    batch = batch or get_client("batch")
    name = f"documents-{utc_timestamp()}"
    response = batch.submit_job(
        jobName=name,
        jobQueue=job_queue,
        jobDefinition=environment(DOCUMENTS_BATCH_JOB_DEFINITION),
    )
    return {**result, "submitted": True, "job_name": name, "job_id": response["jobId"]}


def _sns_councils(event) -> list[str]:
    """The repositories named by the data-commits messages in an SNS event."""
    councils = []
    for record in event.get("Records") or []:
        if record.get("EventSource") != "aws:sns":
            continue
        message = json.loads(record["Sns"]["Message"])
        councils.append(str(message["repository"]).upper())
    return councils


def trigger_handler(event, context=None):
    """
    Find files for the pipeline to do. Called four ways:

    - by the data-commits SNS topic, after a council's scrape pushed:
      queue that council's files that have no status;
    - directly with ``{"council": "STO"}``: the same;
    - with ``{"sweep": true}``, or by EventBridge when the scraping Step
      Function finishes: invoke this function once a council;
    - with ``{"submit_batch": true}``, or by EventBridge's schedule:
      submit a Batch job if the extract queue has work and no job has.
    """
    if event.get("Records"):
        councils = _sns_councils(event)
    elif event.get("council"):
        councils = [str(event["council"]).upper()]
    elif event.get("sweep") or event.get("source") == "aws.states":
        return {"action": "sweep", "councils": sweep(store())}
    elif event.get("submit_batch") or event.get("source") == "aws.events":
        return submit_batch()
    else:
        raise ValueError(f"Not an event the trigger handles: {event!r}")

    targets = Targets.from_environment()
    s3store = store()
    return {
        "action": "queue",
        "queued": {
            council: queue_council(s3store, council, targets) for council in councils
        },
    }


# Dispatch: what a file needs


def route(store, file: File, embedder: str, targets: Targets) -> str:
    """
    Settle a file without Docling if it can be, and send it on if not.
    Returns where it went: ``extract``, ``embed`` or ``ready``. Raises
    if the file isn't in the store or the store failed, so that the
    message is tried again.
    """
    outcome = pipeline.dispatch(store, file)
    if outcome.error:
        raise RuntimeError(outcome.error.strip().splitlines()[-1])
    if outcome.result.missing:
        raise FileNotFoundError(file.document_key)
    if outcome.needs_converter:
        targets.to_extract([file], store)
        return "extract"
    if is_final(outcome.status, embedder):
        targets.ready(file, outcome.status)
        return "ready"
    targets.to_embed([file])
    return "embed"


def _sqs_records(event, work: Callable[[File], object]) -> dict:
    """
    Call ``work`` for each file in an SQS event, and return the records
    that raised as batch item failures, so that SQS tries only those
    again.
    """
    failures = []
    # The same file can arrive twice in a batch: it's worked once, and
    # every one of its records stands or falls with it.
    records: dict[File, list[str]] = {}
    for record in event.get("Records") or []:
        try:
            file = file_from_message(record["body"])
        except (ValueError, KeyError, TypeError):
            logger.exception(f"Message {record.get('messageId')} isn't a file")
            failures.append({"itemIdentifier": record["messageId"]})
            continue
        records.setdefault(file, []).append(record["messageId"])
    for file, message_ids in records.items():
        try:
            work(file)
        except Exception:  # noqa: BLE001
            logger.exception(f"{file.document_key} failed")
            failures += [{"itemIdentifier": id} for id in message_ids]
    return {"batchItemFailures": failures}


def dispatch_handler(event, context=None):
    """SQS event source on the documents queue. See route()."""
    s3store = store()
    targets = Targets.from_environment()
    embedder = embedder_name()
    return _sqs_records(event, lambda file: route(s3store, file, embedder, targets))


# Embed


def embed_file(store, file: File, embedder: EmbedderBase, targets: Targets) -> str:
    """
    Embed a chunked file and publish that it's ready. A file that turns
    out to need Docling (its chunks have gone, or its status says so)
    goes to the extract queue instead. Raises if the embedding isn't
    complete, so that the message is tried again and only the chunks
    that failed are sent.
    """
    plan = pipeline.plan(store, file, pipeline.Options(), [embedder.name])
    if plan.needs_converter:
        targets.to_extract([file], store)
        return "extract"
    if plan.nothing_to_do:
        targets.ready(file, plan.status)
        return "ready"
    [outcome] = pipeline.run_files(
        store,
        [file],
        pipeline.Options(stages=[Stage.EMBED]),
        [embedder],
        plans=[plan],
    )
    if outcome.error:
        raise RuntimeError(outcome.error.strip().splitlines()[-1])
    status = outcome.status
    if not status.chunked:
        # The chunks had gone: the status now says so, and the file
        # needs the converter before it can be embedded.
        targets.to_extract([file], store)
        return "extract"
    if not status.embedded(embedder.name):
        entry = status.embed.get(embedder.name)
        error = (entry.error if entry else None) or "not embedded"
        raise RuntimeError(f"{file.document_key}: {error}")
    targets.ready(file, status)
    return "ready"


def embed_handler(event, context=None):
    """SQS event source on the embed queue. See embed_file()."""
    s3store = store()
    targets = Targets.from_environment()
    embedder = make_embedder()
    return _sqs_records(
        event, lambda file: embed_file(s3store, file, embedder, targets)
    )


# Backfill


def queue_backfill(
    files: Sequence[File],
    max_queued: int = 2000,
    targets: Targets | None = None,
    pause: Callable[[float], None] = time.sleep,
    pause_seconds: float = 30,
    log: Callable[[str], None] | None = None,
) -> int:
    """
    Put files on the documents queue, ten a request, waiting while the
    queue holds more than ``max_queued`` messages so that a backfill
    never swamps the files new scrapes add. Returns how many were sent.
    """
    targets = targets or Targets.from_environment()
    queue_url = targets.documents_queue_url or environment(DOCUMENTS_QUEUE_URL)
    sqs = targets.sqs or get_client("sqs")
    sent = 0
    for batch in batches(list(files)):
        while True:
            visible, _ = queue_depth(queue_url, sqs)
            if visible <= max_queued:
                break
            if log:
                log(
                    f"{visible} messages on the documents queue, more than "
                    f"{max_queued}: waiting {pause_seconds:g}s"
                )
            pause(pause_seconds)
        sent += send_files(queue_url, batch, sqs)
    return sent
