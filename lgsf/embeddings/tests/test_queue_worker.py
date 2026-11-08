"""
The Batch worker's loop: receiving, extracting and chunking, and
settling each message by what became of its file.

As in test_pipeline, extract.convert() is replaced with one that returns
the Manchester decision notice, files are worked on in threads rather
than worker processes, and the files are in a temporary directory. The
queue is a FakeSQS from test_aws.
"""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from lgsf.embeddings import extract, pipeline, queue_worker
from lgsf.embeddings.aws import Targets
from lgsf.embeddings.extract import ExtractStatus
from lgsf.embeddings.files import File
from lgsf.embeddings.chunking import inputs_hash
from lgsf.embeddings.formats import DocumentFormat
from lgsf.embeddings.sidecars import (
    ExtractEntry,
    FileStatus,
    Outcome,
    Stage,
    Suffix,
    read_status,
    result_key,
)
from lgsf.embeddings.stores import LocalStore
from lgsf.embeddings.tests.test_aws import (
    EMBED_QUEUE,
    EXTRACT_QUEUE,
    PDF,
    RESTRICTED,
    TOPIC,
    FakeSNS,
    FakeSQS,
    add_file,
)
from lgsf.storage.documents.sniff import sniff_extension

MAN_DOCLING = (
    Path(__file__).parent / "fixtures" / "MAN" / "decision-notice.docling.json"
)


@pytest.fixture
def converted(monkeypatch):
    docling_core = pytest.importorskip("docling_core.types.doc")
    document = docling_core.DoclingDocument.load_from_json(MAN_DOCLING)
    names = []

    def convert(stream):
        names.append(stream.name)
        return extract.Conversion(ExtractStatus.OK, 2, None, document)

    monkeypatch.setattr(extract, "convert", convert)
    monkeypatch.setattr(extract, "docling_version", lambda: "2.130.0")
    monkeypatch.setattr(extract, "converter_device", lambda: "cpu")
    monkeypatch.setattr(extract, "pdf_title", lambda data: None)
    return names


class BrokenDownloads(LocalStore):
    """A store that can't download one file."""

    def __init__(self, root, broken_key):
        super().__init__(root)
        self.broken_key = broken_key

    def download_to(self, key, directory):
        if key == self.broken_key:
            raise OSError("connection reset")
        return super().download_to(key, directory)


def queue_files(sqs, *files):
    sqs.send_message_batch(
        EXTRACT_QUEUE,
        [
            {
                "Id": str(index),
                "MessageBody": json.dumps(
                    {"council": file.council, "storage_key": file.storage_key}
                ),
            }
            for index, file in enumerate(files)
        ],
    )


def drain(store, sqs, sns=None, **kwargs):
    sns = sns or FakeSNS()
    outcomes = []
    logged = []
    worked = queue_worker.drain(
        EXTRACT_QUEUE,
        store,
        pipeline.Options(),
        on_outcome=outcomes.append,
        log=logged.append,
        targets=Targets(
            embed_queue_url=EMBED_QUEUE, file_ready_topic_arn=TOPIC, sqs=sqs, sns=sns
        ),
        sqs=sqs,
        executor=ThreadPoolExecutor(max_workers=2),
        extend_every=0.01,
        wait_seconds=0,
        **kwargs,
    )
    return worked, outcomes, logged


def test_the_worker_settles_each_file_by_its_outcome(tmp_path, converted):
    pdf = File("STO", hashlib.sha256(PDF).hexdigest() + sniff_extension(PDF))
    store = BrokenDownloads(tmp_path, f"STO/documents/{'e' * 64}.pdf")
    store.write_bytes(pdf.document_key, PDF)
    restricted = add_file(store, RESTRICTED)
    broken = File("STO", "e" * 64 + ".pdf")
    store.write_bytes(broken.document_key, b"%PDF-1.7 whatever")
    sqs = FakeSQS()
    sns = FakeSNS()
    queue_files(sqs, pdf, restricted, broken)

    worked, outcomes, logged = drain(store, sqs, sns, visibility_timeout=600)

    assert worked == 3
    assert {o.file for o in outcomes} == {pdf, restricted, broken}
    # The PDF was extracted and chunked, not embedded, and sent on.
    status = read_status(store, pdf.document_key, pdf.content_hash)
    assert status.chunked
    assert status.embed == {}
    assert sqs.bodies(EMBED_QUEUE) == [
        {"council": "STO", "storage_key": pdf.storage_key}
    ]
    # The restricted page is final: "file ready", nothing to embed.
    assert [(m["storage_key"], m["outcome"]) for m in sns.messages] == [
        (restricted.storage_key, "restricted")
    ]
    # Both were deleted; the file the store couldn't give is left.
    assert sqs.left_in_flight(EXTRACT_QUEUE) == [
        {"council": "STO", "storage_key": broken.storage_key}
    ]
    [outcome] = [o for o in outcomes if o.file == broken]
    assert "connection reset" in outcome.error
    assert read_status(store, broken.document_key, broken.content_hash) is None
    # Every message's visibility was extended while the batch ran.
    extended = {receipt for receipt, _ in sqs.visibility}
    assert len(extended) == 3
    assert all(timeout == 600 for _, timeout in sqs.visibility)
    # Then the queue was empty twice, and the worker stopped.
    receives = [call for call in sqs.calls if call[0] == "receive"]
    assert len(receives) == 3
    assert logged[-1] == "The queue is empty: stopping."
    assert "3 messages received, 3 files" in logged


def test_the_worker_runs_only_extract_and_chunk(tmp_path, converted):
    store = LocalStore(tmp_path)
    pdf = add_file(store, PDF)
    sqs = FakeSQS()
    queue_files(sqs, pdf)

    worked, outcomes, _ = drain(store, sqs)

    [outcome] = outcomes
    assert outcome.embedded_with == []
    assert sorted(
        name.removeprefix(f"{pdf.document_key}.")
        for name in store.list_prefix(f"{pdf.document_key}.")
    ) == ["chunks.jsonl", "docling-v2.json", "md", "status.json"]


def test_an_extracted_file_is_chunked_without_converting(tmp_path, converted):
    """
    A file whose status has an extract entry and no chunk entry, with the
    Docling document beside it, as the copy of lgsf-web's results leaves.
    """
    store = LocalStore(tmp_path)
    pdf = add_file(store, PDF)
    status = FileStatus(
        council=pdf.council,
        storage_key=pdf.storage_key,
        content_hash=pdf.content_hash,
        format=DocumentFormat.PDF,
        outcome=Outcome.OK,
        extract=ExtractEntry(
            status=ExtractStatus.OK,
            tool_version="2.130.0",
            finished_at="2026-10-20T16:02:58+00:00",
            pages=2,
            seconds=1.9,
            device="cuda",
        ),
    )
    store.write_bytes(result_key(pdf.document_key, Suffix.STATUS), status.as_json())
    store.write_bytes(
        result_key(pdf.document_key, Suffix.DOCLING), MAN_DOCLING.read_bytes()
    )
    store.write_bytes(result_key(pdf.document_key, Suffix.TEXT), b"# A report")
    sqs = FakeSQS()
    queue_files(sqs, pdf)

    worked, outcomes, _ = drain(store, sqs)

    assert worked == 1
    assert converted == []
    after = read_status(store, pdf.document_key, pdf.content_hash)
    assert after.chunked
    assert after.extract == status.extract
    assert after.outcome == Outcome.OK
    assert after.chunk.inputs_hash == inputs_hash(
        DocumentFormat.PDF, "2.130.0", pdf.content_hash
    )
    assert store.exists(result_key(pdf.document_key, Suffix.CHUNKS))
    assert sqs.bodies(EMBED_QUEUE) == [
        {"council": "STO", "storage_key": pdf.storage_key}
    ]
    assert sqs.in_flight == {}


def test_a_redelivered_message_for_a_done_file_is_sent_on(tmp_path, converted):
    store = LocalStore(tmp_path)
    pdf = add_file(store, PDF)
    pipeline.run_file(store, pdf, pipeline.Options(stages=[Stage.EXTRACT, Stage.CHUNK]))
    sqs = FakeSQS()
    queue_files(sqs, pdf, pdf)

    worked, outcomes, logged = drain(store, sqs)

    # Worked once, skipped as done, and both messages settled.
    assert worked == 1
    assert "2 messages received, 1 files" in logged
    assert [o.skipped for o in outcomes] == [True]
    assert len(converted) == 1  # the set-up's, none by the worker
    assert len(sqs.bodies(EMBED_QUEUE)) == 1
    assert sqs.in_flight == {}


def test_a_message_that_is_not_a_file_is_left(tmp_path, converted):
    store = LocalStore(tmp_path)
    sqs = FakeSQS()
    sqs.send_message_batch(
        EXTRACT_QUEUE, [{"Id": "0", "MessageBody": '{"council": "STO"}'}]
    )

    worked, outcomes, logged = drain(store, sqs)

    assert worked == 0
    assert outcomes == []
    assert len(sqs.in_flight) == 1
    assert any("not a file message" in line for line in logged)


def test_the_embed_queue_is_required(tmp_path, monkeypatch):
    monkeypatch.delenv("EMBED_QUEUE_URL", raising=False)
    with pytest.raises(OSError, match="EMBED_QUEUE_URL"):
        queue_worker.drain(
            EXTRACT_QUEUE,
            LocalStore(tmp_path),
            pipeline.Options(),
            sqs=FakeSQS(),
            executor=ThreadPoolExecutor(max_workers=1),
        )


class FlakySQS(FakeSQS):
    """Fails the first visibility change, as a throttled client would."""

    def __init__(self):
        super().__init__()
        self.failed = False

    def change_message_visibility(self, **kwargs):
        if not self.failed:
            self.failed = True
            raise RuntimeError("ThrottlingException")
        super().change_message_visibility(**kwargs)


def test_the_extender_carries_on_after_an_error(caplog):
    import time

    sqs = FlakySQS()
    extender = queue_worker.VisibilityExtender(
        sqs, EXTRACT_QUEUE, ["r1"], timeout=600, every=0.01
    )
    # Started without the first extend, so the thread's first round fails.
    extender._thread.start()
    deadline = time.monotonic() + 2
    while len(sqs.visibility) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    extender._stop.set()
    extender._thread.join()

    assert sqs.failed
    assert len(sqs.visibility) >= 2
    assert "Couldn't extend" in caplog.text


def test_disposition():
    file = File("STO", "a" * 64 + ".pdf")
    plan = pipeline.FilePlan(status=None)
    assert queue_worker.disposition(pipeline.FileOutcome(file, plan, error="x")) is None
    assert queue_worker.disposition(pipeline.FileOutcome(file, plan)) is None
