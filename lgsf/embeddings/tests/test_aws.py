"""
The Lambda handlers, the queue messages and "file ready", against
in-memory fakes of SQS, SNS, Lambda, Batch and S3. Nothing runs
Docling: the files are restricted pages, empty files, and PDFs that
only ever get as far as being routed.
"""

import datetime
import hashlib
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from dataclasses import replace
from uuid import uuid4

import pytest

from lgsf.embeddings import aws
from lgsf.embeddings.aws import Targets
from lgsf.embeddings.chunking import inputs_hash
from lgsf.embeddings.embedders import EMBEDDERS, Embedding, Failed, FakeEmbedder
from lgsf.embeddings.extract import ExtractStatus
from lgsf.embeddings.files import File
from lgsf.embeddings.formats import DocumentFormat
from lgsf.embeddings.records import Chunk, chunks_jsonl
from lgsf.embeddings.sidecars import (
    ChunkEntry,
    EmbedEntry,
    ExtractEntry,
    FileStatus,
    Outcome,
    Suffix,
    read_status,
    result_key,
)
from lgsf.embeddings.stores import S3Store
from lgsf.storage.documents.sniff import sniff_extension
from lgsf.storage.tests.test_s3_storage import FakeS3

BUCKET = "lgsf-bucket"
PREFIX = "merged-data"
DOCUMENTS_QUEUE = "https://sqs.eu-west-2.amazonaws.com/1/documents"
EXTRACT_QUEUE = "https://sqs.eu-west-2.amazonaws.com/1/extract"
EMBED_QUEUE = "https://sqs.eu-west-2.amazonaws.com/1/embed"
TOPIC = "arn:aws:sns:eu-west-2:1:file-ready"

PDF = b"%PDF-1.7\nA made-up PDF"
RESTRICTED = b"<html><h1>Reasons restricted</h1></html>"
LOGON = b"<html><body>Logon</body></html>"


# Fakes


class FakeSQS:
    """
    Queues of messages, with the messages received but not deleted kept
    in flight until redeliver() puts them back, as a visibility timeout
    would.
    """

    def __init__(self):
        self.queues = defaultdict(list)
        self.in_flight = {}
        self.calls = []
        self.visibility = []

    def send_message_batch(self, QueueUrl, Entries):
        self.calls.append(("send", QueueUrl, len(Entries)))
        for entry in Entries:
            self.queues[QueueUrl].append(
                {"MessageId": uuid4().hex, "Body": entry["MessageBody"]}
            )
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}

    def receive_message(self, QueueUrl, MaxNumberOfMessages=1, WaitTimeSeconds=0):
        self.calls.append(("receive", QueueUrl))
        messages = []
        while self.queues[QueueUrl] and len(messages) < MaxNumberOfMessages:
            message = self.queues[QueueUrl].pop(0)
            receipt = uuid4().hex
            self.in_flight[receipt] = (QueueUrl, message)
            messages.append({**message, "ReceiptHandle": receipt})
        return {"Messages": messages} if messages else {}

    def delete_message_batch(self, QueueUrl, Entries):
        self.calls.append(("delete", QueueUrl, len(Entries)))
        for entry in Entries:
            self.in_flight.pop(entry["ReceiptHandle"], None)
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}

    def change_message_visibility(self, QueueUrl, ReceiptHandle, VisibilityTimeout):
        self.visibility.append((ReceiptHandle, VisibilityTimeout))

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        self.calls.append(("attributes", QueueUrl))
        in_flight = sum(1 for url, _ in self.in_flight.values() if url == QueueUrl)
        return {
            "Attributes": {
                "ApproximateNumberOfMessages": str(len(self.queues[QueueUrl])),
                "ApproximateNumberOfMessagesNotVisible": str(in_flight),
            }
        }

    def redeliver(self):
        for receipt, (url, message) in list(self.in_flight.items()):
            self.queues[url].append(message)
            del self.in_flight[receipt]

    def bodies(self, queue_url):
        return [json.loads(m["Body"]) for m in self.queues[queue_url]]

    def left_in_flight(self, queue_url):
        return [
            json.loads(m["Body"])
            for url, m in self.in_flight.values()
            if url == queue_url
        ]


class FakeSNS:
    def __init__(self):
        self.published = []

    def publish(self, **kwargs):
        self.published.append(kwargs)

    @property
    def messages(self):
        return [json.loads(p["Message"]) for p in self.published]


class FakeLambda:
    def __init__(self):
        self.invocations = []

    def invoke(self, FunctionName, InvocationType, Payload):
        self.invocations.append((FunctionName, InvocationType, json.loads(Payload)))
        return {"StatusCode": 202}


class FakeBatch:
    def __init__(self, active=()):
        self.active = list(active)
        self.submitted = []

    def list_jobs(self, jobQueue, jobStatus):
        return {
            "jobSummaryList": [
                {"jobName": name, "status": status}
                for name, status in self.active
                if status == jobStatus
            ]
        }

    def submit_job(self, jobName, jobQueue, jobDefinition):
        self.submitted.append((jobName, jobQueue, jobDefinition))
        return {"jobId": "job-1", "jobName": jobName}


class Clients:
    def __init__(self):
        self.s3 = FakeS3()
        self.sqs = FakeSQS()
        self.sns = FakeSNS()
        self.lambda_ = FakeLambda()
        self.batch = FakeBatch()

    def get(self, service):
        return {
            "s3": self.s3,
            "sqs": self.sqs,
            "sns": self.sns,
            "lambda": self.lambda_,
            "batch": self.batch,
        }[service]

    @property
    def store(self):
        return S3Store(BUCKET, PREFIX, client=self.s3)


@pytest.fixture
def clients(monkeypatch):
    """The environment a Lambda has, and fakes in place of its clients."""
    fakes = Clients()
    monkeypatch.setattr(aws, "get_client", fakes.get)
    monkeypatch.setenv("LGSF_S3_BUCKET", BUCKET)
    monkeypatch.setenv("LGSF_S3_PREFIX", PREFIX)
    monkeypatch.setenv(aws.DOCUMENTS_QUEUE_URL, DOCUMENTS_QUEUE)
    monkeypatch.setenv(aws.EXTRACT_QUEUE_URL, EXTRACT_QUEUE)
    monkeypatch.setenv(aws.EMBED_QUEUE_URL, EMBED_QUEUE)
    monkeypatch.setenv(aws.FILE_READY_TOPIC_ARN, TOPIC)
    monkeypatch.setenv(aws.DOCUMENTS_BATCH_JOB_QUEUE, "documents-job-queue")
    monkeypatch.setenv(aws.DOCUMENTS_BATCH_JOB_DEFINITION, "documents-job-definition")
    monkeypatch.setenv(aws.TRIGGER_FUNCTION_NAME, "documents-trigger")
    monkeypatch.setenv(aws.EMBEDDER, "fake")
    monkeypatch.setenv(aws.DOCUMENTS_SINCE, "2026-01-01T00:00:00Z")
    return fakes


def add_file(store, data, council="STO") -> File:
    file = File(council, hashlib.sha256(data).hexdigest() + sniff_extension(data))
    store.write_bytes(file.document_key, data)
    return file


def chunked_status(file, embedder=None, embed_error=None) -> FileStatus:
    """The status of a PDF that's extracted and chunked, and perhaps embedded."""
    status = FileStatus(
        council=file.council,
        storage_key=file.storage_key,
        content_hash=file.content_hash,
        format=DocumentFormat.PDF,
        outcome=Outcome.OK,
        extract=ExtractEntry(status=ExtractStatus.OK, tool_version="2.130.0"),
    )
    chunk_hash = inputs_hash(DocumentFormat.PDF, "2.130.0", file.content_hash)
    status.chunk = ChunkEntry(inputs_hash=chunk_hash, chunks=2, embeddable=2)
    if embedder:
        status.embed[embedder] = EmbedEntry(
            model="fake-sha256", chunk_inputs_hash=chunk_hash, error=embed_error
        )
    return status


def add_chunked(store, data, texts, **status_args) -> File:
    """A PDF with its status, and chunks of ``texts``."""
    file = add_file(store, data)
    status = chunked_status(file, **status_args)
    store.write_bytes(result_key(file.document_key, Suffix.STATUS), status.as_json())
    # Stands in for the Docling document, which nothing here reads.
    store.write_bytes(result_key(file.document_key, Suffix.DOCLING), b"{}")
    chunks = [
        Chunk(
            chunk_id=f"{file.council}:documents:{file.digest}:{index}",
            council=file.council,
            content_hash=file.content_hash,
            storage_key=file.storage_key,
            chunk_index=index,
            chunk_count=len(texts),
            header="A report",
            text=text,
            embed=True,
        )
        for index, text in enumerate(texts)
    ]
    store.write_bytes(
        result_key(file.document_key, Suffix.CHUNKS), chunks_jsonl(chunks)
    )
    return file


def sqs_event(*files, bodies=()):
    records = [
        {"messageId": f"m{index}", "body": json.dumps(aws.message(file))}
        for index, file in enumerate(files)
    ]
    records += [
        {"messageId": f"b{index}", "body": body} for index, body in enumerate(bodies)
    ]
    return {"Records": records}


# Messages


def test_a_message_is_a_council_and_a_storage_key():
    file = File("STO", "a" * 64 + ".pdf")
    body = json.dumps(aws.message(file))
    assert json.loads(body) == {"council": "STO", "storage_key": "a" * 64 + ".pdf"}
    assert aws.file_from_message(body) == file
    assert aws.file_from_message({"council": "sto", "storage_key": file.storage_key})
    for bad in ('{"council": "STO"}', '{"council": "STO", "storage_key": "x.pdf"}'):
        with pytest.raises(ValueError):
            aws.file_from_message(bad)


def test_files_are_sent_ten_a_request(clients):
    files = [File("STO", f"{i:064x}.pdf") for i in range(23)]
    assert aws.send_files(DOCUMENTS_QUEUE, files, clients.sqs) == 23
    assert [call[2] for call in clients.sqs.calls if call[0] == "send"] == [10, 10, 3]
    assert clients.sqs.bodies(DOCUMENTS_QUEUE)[0] == aws.message(files[0])


def test_a_refused_message_raises(clients):
    def refuse(QueueUrl, Entries):
        return {"Failed": [{"Id": "0", "Code": "Throttled"}]}

    clients.sqs.send_message_batch = refuse
    with pytest.raises(RuntimeError, match="Throttled"):
        aws.send_files(DOCUMENTS_QUEUE, [File("STO", "a" * 64 + ".pdf")], clients.sqs)


def test_file_ready_message(clients):
    file = File("STO", "a" * 64 + ".pdf")
    status = replace(chunked_status(file), outcome=Outcome.RESTRICTED)
    targets = Targets.from_environment(sns=clients.sns)
    assert targets.ready(file, status)
    assert clients.sns.published[0]["TopicArn"] == TOPIC
    assert clients.sns.messages == [
        {
            "council": "STO",
            "storage_key": file.storage_key,
            "content_hash": f"sha256:{'a' * 64}",
            "outcome": "restricted",
            "status_key": f"STO/documents/{file.storage_key}.status.json",
            "schema_version": 1,
        }
    ]


def test_file_ready_is_skipped_without_a_topic(clients, monkeypatch):
    monkeypatch.delenv(aws.FILE_READY_TOPIC_ARN)
    file = File("STO", "a" * 64 + ".pdf")
    assert not Targets.from_environment().ready(file, chunked_status(file))
    assert clients.sns.published == []


def test_a_queue_that_is_not_set_raises(monkeypatch):
    monkeypatch.delenv(aws.EXTRACT_QUEUE_URL, raising=False)
    with pytest.raises(OSError, match=aws.EXTRACT_QUEUE_URL):
        Targets.from_environment().to_extract([File("STO", "a" * 64 + ".pdf")])


def test_is_final():
    file = File("STO", "a" * 64 + ".pdf")
    assert not aws.is_final(None, "fake")
    assert not aws.is_final(chunked_status(file), "fake")
    assert aws.is_final(chunked_status(file, "fake"), "fake")
    assert not aws.is_final(chunked_status(file, "fake", "1 of 2 not embedded"), "fake")
    assert aws.is_final(replace(chunked_status(file), outcome=Outcome.FAILED), "fake")


# The trigger


def sns_event(*councils):
    return {
        "Records": [
            {
                "EventSource": "aws:sns",
                "Sns": {
                    "Message": json.dumps(
                        {
                            "repository": council,
                            "after": "abc123",
                            "scraper_type": "minutes",
                            "run_id": "r1",
                        }
                    )
                },
            }
            for council in councils
        ]
    }


@pytest.fixture
def two_councils(clients):
    """STO with a PDF, a restricted page and a done PDF; OLD with one PDF."""
    store = clients.store
    pdf = add_file(store, PDF)
    restricted = add_file(store, RESTRICTED)
    done = add_file(store, PDF + b"\n% done")
    store.write_bytes(
        result_key(done.document_key, Suffix.STATUS), chunked_status(done).as_json()
    )
    # Something that isn't a document at all is left alone.
    store.write_bytes("STO/Minutes/json/2026-01-01-1.json", b"{}")
    other = add_file(store, PDF + b"\n% old", council="OLD")
    return {"pdf": pdf, "restricted": restricted, "done": done, "other": other}


def test_an_sns_message_queues_the_councils_files_without_a_status(
    clients, two_councils
):
    result = aws.trigger_handler(sns_event("STO"), None)

    assert result == {"action": "queue", "queued": {"STO": 2}}
    queued = clients.sqs.bodies(DOCUMENTS_QUEUE)
    assert sorted(q["storage_key"] for q in queued) == sorted(
        two_councils[name].storage_key for name in ("pdf", "restricted")
    )
    assert all(q["council"] == "STO" for q in queued)


def mark(clients, file, age: datetime.timedelta):
    """A queued marker beside ``file``, written ``age`` ago."""
    aws.mark_queued(clients.store, file)
    key = f"{PREFIX}/{result_key(file.document_key, aws.QUEUED_SUFFIX)}"
    clients.s3.objects[key]["LastModified"] = datetime.datetime.now(datetime.UTC) - age


def test_a_file_waiting_on_the_extract_queue_is_not_queued_again(clients, two_councils):
    mark(clients, two_councils["pdf"], datetime.timedelta(hours=1))

    result = aws.trigger_handler({"council": "STO"}, None)

    assert result["queued"] == {"STO": 1}
    assert clients.sqs.bodies(DOCUMENTS_QUEUE) == [
        aws.message(two_councils["restricted"])
    ]


def test_a_stale_marker_is_ignored(clients, two_councils):
    mark(clients, two_councils["pdf"], datetime.timedelta(days=8))

    result = aws.trigger_handler({"council": "STO"}, None)

    assert result["queued"] == {"STO": 2}


def written_at(clients, file, when):
    clients.s3.objects[f"{PREFIX}/{file.document_key}"]["LastModified"] = when


def test_files_from_before_documents_since_are_left_to_the_backfill(
    clients, two_councils, monkeypatch
):
    since = datetime.datetime(2026, 11, 9, tzinfo=datetime.UTC)
    monkeypatch.setenv(aws.DOCUMENTS_SINCE, "2026-11-09T00:00:00Z")
    written_at(clients, two_councils["pdf"], since - datetime.timedelta(seconds=1))
    written_at(clients, two_councils["restricted"], since)

    result = aws.trigger_handler({"council": "STO"}, None)

    assert result["queued"] == {"STO": 1}
    assert clients.sqs.bodies(DOCUMENTS_QUEUE) == [
        aws.message(two_councils["restricted"])
    ]


def test_without_documents_since_the_trigger_queues_nothing(
    clients, two_councils, monkeypatch, caplog
):
    monkeypatch.delenv(aws.DOCUMENTS_SINCE)

    result = aws.trigger_handler(sns_event("STO"), None)

    assert result == {"action": "queue", "queued": {"STO": 0}}
    assert clients.sqs.bodies(DOCUMENTS_QUEUE) == []
    assert aws.DOCUMENTS_SINCE in caplog.text


def test_documents_since_is_read_as_utc(monkeypatch):
    monkeypatch.setenv(aws.DOCUMENTS_SINCE, "2026-11-09T01:00:00+01:00")
    assert aws.documents_since() == datetime.datetime(2026, 11, 9, tzinfo=datetime.UTC)
    monkeypatch.setenv(aws.DOCUMENTS_SINCE, "2026-11-09")
    assert aws.documents_since().tzinfo is datetime.UTC
    monkeypatch.setenv(aws.DOCUMENTS_SINCE, "yesterday")
    with pytest.raises(ValueError):
        aws.documents_since()


def test_a_direct_council_queues_the_same(clients, two_councils):
    assert aws.trigger_handler({"council": "old"}, None) == {
        "action": "queue",
        "queued": {"OLD": 1},
    }
    assert clients.sqs.bodies(DOCUMENTS_QUEUE) == [aws.message(two_councils["other"])]


def test_a_sweep_invokes_the_trigger_once_a_council(clients, two_councils):
    result = aws.trigger_handler({"sweep": True}, None)

    assert result == {"action": "sweep", "councils": ["OLD", "STO"]}
    assert clients.lambda_.invocations == [
        ("documents-trigger", "Event", {"council": "OLD"}),
        ("documents-trigger", "Event", {"council": "STO"}),
    ]
    assert clients.sqs.bodies(DOCUMENTS_QUEUE) == []


def test_a_step_function_finishing_is_a_sweep(clients, two_councils):
    event = {
        "source": "aws.states",
        "detail-type": "Step Functions Execution Status Change",
        "detail": {"status": "SUCCEEDED"},
    }
    assert aws.trigger_handler(event, None)["action"] == "sweep"
    assert len(clients.lambda_.invocations) == 2


def test_submit_batch_with_an_empty_queue(clients):
    result = aws.trigger_handler({"submit_batch": True}, None)
    assert result["submitted"] is False
    assert result["visible"] == 0
    assert "empty" in result["reason"]
    assert clients.batch.submitted == []


def test_submit_batch_submits_one_job(clients):
    aws.send_files(EXTRACT_QUEUE, [File("STO", "a" * 64 + ".pdf")], clients.sqs)

    result = aws.trigger_handler({"submit_batch": True}, None)

    assert result["submitted"] is True
    assert result["visible"] == 1
    assert result["job_id"] == "job-1"
    [(name, queue, definition)] = clients.batch.submitted
    assert name.startswith("documents-20")
    assert name == result["job_name"]
    assert (queue, definition) == ("documents-job-queue", "documents-job-definition")


def test_submit_batch_leaves_a_running_job_to_it(clients):
    aws.send_files(EXTRACT_QUEUE, [File("STO", "a" * 64 + ".pdf")], clients.sqs)
    clients.batch.active = [("documents-earlier", "RUNNING")]

    result = aws.trigger_handler({"submit_batch": True}, None)

    assert result["submitted"] is False
    assert "documents-earlier" in result["reason"]
    assert clients.batch.submitted == []


def test_a_scheduled_event_is_submit_batch(clients):
    event = {"source": "aws.events", "detail-type": "Scheduled Event"}
    assert aws.trigger_handler(event, None)["action"] == "submit_batch"


def test_an_unknown_event_is_refused(clients):
    with pytest.raises(ValueError):
        aws.trigger_handler({"something": "else"}, None)


# Dispatch


def test_dispatch_routes_each_outcome(clients):
    store = clients.store
    restricted = add_file(store, RESTRICTED)
    empty = add_file(store, b"")
    logon = add_file(store, LOGON)
    changed = File("STO", "c" * 64 + ".pdf")
    store.write_bytes(changed.document_key, b"%PDF-1.7 other bytes")
    pdf = add_file(store, PDF)
    chunked = add_chunked(store, PDF + b"\n% chunked", ["one", "two"])
    partly = add_chunked(
        store,
        PDF + b"\n% partly",
        ["one", "two"],
        embedder="fake",
        embed_error="1 of 2 chunks not embedded: throttled",
    )
    done = add_chunked(store, PDF + b"\n% done", ["one", "two"], embedder="fake")
    failed = add_file(store, PDF + b"\n% failed")
    store.write_bytes(
        result_key(failed.document_key, Suffix.STATUS),
        replace(
            chunked_status(failed),
            outcome=Outcome.FAILED,
            extract=ExtractEntry(status=ExtractStatus.FAILED),
            chunk=None,
        ).as_json(),
    )

    result = aws.dispatch_handler(
        sqs_event(
            restricted, empty, logon, changed, pdf, chunked, partly, done, failed
        ),
        None,
    )

    assert result == {"batchItemFailures": []}
    assert clients.sqs.bodies(EXTRACT_QUEUE) == [aws.message(pdf)]
    assert clients.sqs.bodies(EMBED_QUEUE) == [
        aws.message(chunked),
        aws.message(partly),
    ]
    ready = {m["storage_key"]: m["outcome"] for m in clients.sns.messages}
    assert ready == {
        restricted.storage_key: "restricted",
        empty.storage_key: "empty",
        logon.storage_key: "not_a_document",
        changed.storage_key: "failed",
        done.storage_key: "ok",
        failed.storage_key: "failed",
    }
    # The cheap outcomes were recorded, and the PDF wasn't touched
    # apart from being marked as queued.
    for file in (restricted, empty, logon, changed):
        assert read_status(store, file.document_key, file.content_hash) is not None
    assert read_status(store, pdf.document_key, pdf.content_hash) is None
    marker = json.loads(store.read_bytes(result_key(pdf.document_key, "queued.json")))
    assert marker["queue"] == "extract"
    assert datetime.datetime.fromisoformat(marker["queued_at"]).tzinfo is not None
    for file in (restricted, chunked, done):
        assert not store.exists(result_key(file.document_key, "queued.json"))


def test_dispatch_reports_the_records_that_failed(clients):
    store = clients.store
    restricted = add_file(store, RESTRICTED)
    missing = File("STO", "d" * 64 + ".pdf")

    result = aws.dispatch_handler(
        sqs_event(restricted, missing, bodies=["not json", '{"council": "STO"}']),
        None,
    )

    failed = sorted(f["itemIdentifier"] for f in result["batchItemFailures"])
    assert failed == ["b0", "b1", "m1"]
    assert [m["storage_key"] for m in clients.sns.messages] == [restricted.storage_key]


def test_dispatch_works_a_file_delivered_twice_once(clients):
    store = clients.store
    pdf = add_file(store, PDF)
    missing = File("STO", "d" * 64 + ".pdf")

    result = aws.dispatch_handler(sqs_event(pdf, pdf, missing, missing), None)

    assert result == {
        "batchItemFailures": [{"itemIdentifier": "m2"}, {"itemIdentifier": "m3"}]
    }
    assert clients.sqs.bodies(EXTRACT_QUEUE) == [aws.message(pdf)]


def test_dispatch_a_store_error_is_a_failure(clients):
    store = clients.store
    restricted = add_file(store, RESTRICTED)
    clients.s3.fail_on = "status.json"

    result = aws.dispatch_handler(sqs_event(restricted), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m0"}]}
    assert clients.sns.messages == []


# Embed


def test_embed_handler_embeds_and_publishes(clients):
    store = clients.store
    file = add_chunked(store, PDF, ["one two three", "four five six"])

    result = aws.embed_handler(sqs_event(file), None)

    assert result == {"batchItemFailures": []}
    status = read_status(store, file.document_key, file.content_hash)
    assert status.embedded("fake")
    assert status.embed["fake"].embedded == 2
    assert store.exists(result_key(file.document_key, "fake.jsonl"))
    assert clients.sns.messages == [
        {
            "council": "STO",
            "storage_key": file.storage_key,
            "content_hash": file.content_hash,
            "outcome": "ok",
            "status_key": result_key(file.document_key, Suffix.STATUS),
            "schema_version": 1,
        }
    ]


class HalfEmbedder(FakeEmbedder):
    """Fails every other text, as a throttled Bedrock would."""

    def embed_documents(self, texts):
        return [
            Embedding(vector=self.vector(text), token_count=1)
            if index % 2 == 0
            else Failed("ThrottlingException")
            for index, text in enumerate(texts)
        ]


def test_a_partial_embedding_is_a_failure_to_try_again(clients, monkeypatch):
    monkeypatch.setitem(EMBEDDERS, "fake", HalfEmbedder)
    store = clients.store
    file = add_chunked(store, PDF, ["one two three", "four five six"])

    result = aws.embed_handler(sqs_event(file), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m0"}]}
    status = read_status(store, file.document_key, file.content_hash)
    assert status.embed["fake"].error.startswith("1 of 2 chunks not embedded")
    assert clients.sns.messages == []

    # The next delivery, once Bedrock is back, sends only the failed chunk.
    monkeypatch.setitem(EMBEDDERS, "fake", FakeEmbedder)
    assert aws.embed_handler(sqs_event(file), None) == {"batchItemFailures": []}
    status = read_status(store, file.document_key, file.content_hash)
    assert status.embed["fake"].embedded == 1
    assert status.embed["fake"].reused == 1
    assert [m["outcome"] for m in clients.sns.messages] == ["ok"]


def test_embed_handler_publishes_for_a_file_already_done(clients):
    store = clients.store
    file = add_chunked(store, PDF, ["one two three"], embedder="fake")

    assert aws.embed_handler(sqs_event(file), None) == {"batchItemFailures": []}
    assert [m["outcome"] for m in clients.sns.messages] == ["ok"]
    assert not store.exists(result_key(file.document_key, "fake.jsonl"))


def test_embed_handler_sends_a_file_whose_chunks_have_gone_to_extract(clients):
    store = clients.store
    file = add_chunked(store, PDF, ["one two three"])
    clients.s3.objects.pop(f"{PREFIX}/{result_key(file.document_key, Suffix.CHUNKS)}")

    assert aws.embed_handler(sqs_event(file), None) == {"batchItemFailures": []}
    status = read_status(store, file.document_key, file.content_hash)
    assert status.chunk is None
    assert clients.sns.messages == []
    assert clients.sqs.bodies(EXTRACT_QUEUE) == [aws.message(file)]
    assert store.exists(result_key(file.document_key, aws.QUEUED_SUFFIX))
    # The status now says so too, so a repeated delivery goes the same way.
    assert aws.embed_handler(sqs_event(file), None) == {"batchItemFailures": []}
    assert len(clients.sqs.bodies(EXTRACT_QUEUE)) == 2


def test_the_embedder_comes_from_the_environment(clients, monkeypatch):
    monkeypatch.delenv(aws.EMBEDDER)
    assert aws.embedder_name() == "titan-v2"
    monkeypatch.setenv(aws.EMBEDDER, "fake")
    assert isinstance(aws.make_embedder(), FakeEmbedder)


# Downloading


def test_download_to_streams_the_file(clients, tmp_path):
    store = clients.store
    data = b"%PDF-1.7\n" + b"x" * (10 * 1024 * 1024)
    file = add_file(store, data)

    path = store.download_to(file.document_key, tmp_path)

    assert path.read_bytes() == data
    assert clients.s3.ops("download") == [f"{PREFIX}/{file.document_key}"]
    assert clients.s3.ops("get") == []
    with pytest.raises(FileNotFoundError):
        store.download_to("STO/documents/missing.pdf", tmp_path)


def test_inspect_never_holds_the_file_in_memory(clients, tmp_path, monkeypatch):
    from lgsf.embeddings import pipeline

    store = clients.store
    data = b"%PDF-1.7\n" + b"x" * (10 * 1024 * 1024)
    file = add_file(store, data)

    def no_read(*args):
        raise AssertionError("reading the whole file would hold it in memory")

    monkeypatch.setattr(store, "read_bytes", no_read)
    monkeypatch.setattr(Path, "read_bytes", no_read)

    result, path = pipeline._inspect(file, store, tmp_path)

    assert result.format == "pdf"
    assert result.extract is None
    assert path.stat().st_size == len(data)


# Backfill


def test_backfill_waits_while_the_queue_is_full(clients):
    files = [File("STO", f"{i:064x}.pdf") for i in range(25)]
    # Ten messages already waiting, and a limit of fifteen: the second
    # batch would take it over, so the backfill waits until they drain.
    aws.send_files(DOCUMENTS_QUEUE, files[:10], clients.sqs)
    paused = []

    def consume(seconds):
        paused.append(seconds)
        del clients.sqs.queues[DOCUMENTS_QUEUE][:10]

    sent = aws.queue_backfill(
        files[10:],
        max_queued=15,
        targets=Targets.from_environment(sqs=clients.sqs),
        pause=consume,
        pause_seconds=5,
        log=lambda text: None,
    )

    assert sent == 15
    assert paused == [5]
    assert len(clients.sqs.bodies(DOCUMENTS_QUEUE)) == 15
    sends = [call[2] for call in clients.sqs.calls if call[0] == "send"]
    assert sends == [10, 10, 5]


# Without Docling


def test_aws_imports_without_docling():
    """
    The Lambdas run with boto3 alone. Checked in a fresh interpreter, so
    that re-importing the package doesn't disturb this process's modules.
    """
    script = (
        "import sys\n"
        "for name in ('docling', 'docling_core', 'pypdfium2', 'torch', 'pandas'):\n"
        "    sys.modules[name] = None\n"
        "from lgsf.embeddings import aws, queue_worker\n"
        "assert aws.trigger_handler and aws.dispatch_handler and aws.embed_handler\n"
        "assert queue_worker.drain\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).parents[3],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
