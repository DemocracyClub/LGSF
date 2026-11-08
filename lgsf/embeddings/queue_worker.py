"""
The Batch worker: extract and chunk the files on the extract queue.

``manage.py embeddings --drain-queue <url>`` calls drain(). It runs on a
Batch instance with Docling installed, receives up to ten messages at a
time, runs the extract and chunk stages for their files with
pipeline.run_files() in a pool of worker processes, and then settles
each message by what became of its file:

- chunked: the file goes to the embed queue, and the message is deleted;
- left out or failed (restricted, empty, not a document, Docling
  failed): "file ready" is published, and the message is deleted;
- anything else (the store failed, the file isn't there, chunking
  failed): the message is left, so SQS delivers it again and in the end
  dead-letters it.

While a batch converts, the messages' visibility is extended every few
minutes, so a long document doesn't reappear on the queue before it's
done. The worker exits when the queue has been empty for two receives.
"""

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from lgsf.embeddings import aws, pipeline
from lgsf.embeddings.aws import FINAL_WITHOUT_EMBEDDING, BATCH_SIZE, Targets
from lgsf.embeddings.extract import worker_pool
from lgsf.embeddings.files import File
from lgsf.embeddings.pipeline import FileOutcome
from lgsf.embeddings.sidecars import Stage

logger = logging.getLogger(__name__)

#: The stages this worker runs. Embedding is the embed Lambda's.
STAGES = (Stage.EXTRACT, Stage.CHUNK)

#: How long a received message stays off the queue, and how often that's
#: renewed while its file is being worked on. SQS allows at most twelve
#: hours in all, far more than a file's conversion timeout.
VISIBILITY_TIMEOUT = 600
EXTEND_EVERY = 240

#: Long polling: how long one receive waits for a message.
WAIT_SECONDS = 20

#: Consecutive empty receives before the worker exits.
EMPTY_RECEIVES = 2


class VisibilityExtender:
    """
    Keeps a batch's messages invisible while they're worked on: extends
    them once at once, then every ``every`` seconds until stopped.
    """

    def __init__(self, sqs, queue_url, receipts, timeout, every):
        self.sqs = sqs
        self.queue_url = queue_url
        self.receipts = list(receipts)
        self.timeout = timeout
        self.every = every
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def extend(self):
        for receipt in self.receipts:
            self.sqs.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt,
                VisibilityTimeout=self.timeout,
            )

    def _run(self):
        while not self._stop.wait(self.every):
            try:
                self.extend()
            except Exception:  # noqa: BLE001
                # A throttle or a blip. The next round tries again; a
                # message that does become visible is worked twice, not
                # lost.
                logger.exception("Couldn't extend the messages' visibility")

    def __enter__(self):
        self.extend()
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._stop.set()
        self._thread.join()


def plan_batch(store, files, options, threads=16) -> list:
    """
    One plan a file, from each file's own status. A batch is ten files
    of perhaps thousands in a council, so its statuses are read one by
    one rather than from a listing of the whole directory.
    """
    with ThreadPoolExecutor(max_workers=threads) as pool:
        return list(
            pool.map(lambda file: pipeline.plan(store, file, options, []), files)
        )


def disposition(outcome: FileOutcome) -> str | None:
    """
    What to do with a worked file's messages: ``embed`` (send the file
    on and delete them), ``ready`` (publish and delete them), or None
    to leave them for SQS to deliver again.
    """
    if outcome.error or outcome.result.missing or outcome.result.too_long:
        return None
    status = outcome.status
    if status is None:
        return None
    if status.chunked:
        return "embed"
    if status.outcome in FINAL_WITHOUT_EMBEDDING:
        return "ready"
    return None


def drain(
    queue_url: str,
    store,
    options: pipeline.Options,
    workers: int = 2,
    on_outcome: Callable[[FileOutcome], None] | None = None,
    log: Callable[[str], None] | None = None,
    targets: Targets | None = None,
    sqs=None,
    executor=None,
    visibility_timeout: float = VISIBILITY_TIMEOUT,
    extend_every: float = EXTEND_EVERY,
    wait_seconds: int = WAIT_SECONDS,
) -> int:
    """
    Work the extract queue until it's been empty for two receives.
    ``on_outcome`` is called for each file as the pipeline finishes it,
    as run_files() does. Returns how many files were worked on.

    The embed queue is required (``targets`` or EMBED_QUEUE_URL): the
    worker's output goes there.
    """
    options = replace(options, stages=list(STAGES))
    sqs = sqs or aws.get_client("sqs")
    targets = targets or Targets.from_environment(sqs=sqs)
    if not targets.embed_queue_url:
        raise OSError(f"{aws.EMBED_QUEUE_URL} isn't set")
    log = log or (lambda text: None)
    executor = executor or worker_pool(workers, options.converter)

    worked = 0
    empty = 0
    with executor:
        while empty < EMPTY_RECEIVES:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=BATCH_SIZE,
                WaitTimeSeconds=wait_seconds,
            )
            messages = response.get("Messages") or []
            if not messages:
                empty += 1
                continue
            empty = 0
            worked += _work_batch(
                queue_url,
                messages,
                store,
                options,
                executor,
                on_outcome,
                log,
                targets,
                sqs,
                visibility_timeout,
                extend_every,
            )
    log("The queue is empty: stopping.")
    return worked


def _work_batch(
    queue_url,
    messages,
    store,
    options,
    executor,
    on_outcome,
    log,
    targets,
    sqs,
    visibility_timeout,
    extend_every,
) -> int:
    # The same file can arrive twice: it's worked once, and every one
    # of its messages is settled together.
    receipts: dict[File, list[str]] = {}
    for item in messages:
        try:
            file = aws.file_from_message(item["Body"])
        except (ValueError, KeyError) as error:
            log(f"Left on the queue, not a file message: {error}")
            continue
        receipts.setdefault(file, []).append(item["ReceiptHandle"])
    files = sorted(receipts)
    if not files:
        return 0
    log(f"{len(messages)} messages received, {len(files)} files")

    all_receipts = [r for handles in receipts.values() for r in handles]
    with VisibilityExtender(
        sqs, queue_url, all_receipts, visibility_timeout, extend_every
    ):
        plans = plan_batch(store, files, options)
        outcomes = pipeline.run_files(
            store, files, options, (), executor, on_outcome, plans=plans
        )

    to_embed = []
    settled = []
    for outcome in outcomes:
        what = disposition(outcome)
        if what == "embed":
            to_embed.append(outcome.file)
        elif what == "ready":
            targets.ready(outcome.file, outcome.status)
        else:
            continue
        settled.append(outcome.file)
    # Sent on before the messages are deleted, so a stop in between
    # repeats a file rather than losing it.
    targets.to_embed(to_embed)
    _delete(sqs, queue_url, [r for file in settled for r in receipts[file]], log)
    return len(files)


def _delete(sqs, queue_url, receipts, log):
    for batch in aws.batches(receipts):
        response = sqs.delete_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": str(index), "ReceiptHandle": receipt}
                for index, receipt in enumerate(batch)
            ],
        )
        for failed in response.get("Failed") or []:
            log(f"Couldn't delete a message: {failed.get('Message') or failed}")
