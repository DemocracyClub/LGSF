"""
``manage.py embeddings``: choosing files, --dry-run, and a run against
made-up files with a fake converter.
"""

import hashlib
import io
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from lgsf.embeddings import commands, extract, queue_worker
from lgsf.embeddings.commands import Command
from lgsf.embeddings.extract import ExtractStatus
from lgsf.embeddings.sidecars import read_status
from lgsf.embeddings.stores import LocalStore
from lgsf.storage.documents.sniff import sniff_extension

RESTRICTED = b"<html><h1>Reasons restricted</h1></html>"
PDF = b"%PDF-1.7\nA made-up PDF"


def key_for(data):
    return hashlib.sha256(data).hexdigest() + sniff_extension(data)


@pytest.fixture
def store(tmp_path, monkeypatch):
    """
    A data directory with two councils: STO has a PDF linked from a 2026
    meeting and a restricted page linked from a 2025 decision, OLD an
    empty file.
    """
    store = LocalStore(tmp_path)
    monkeypatch.setattr(commands, "store_for", lambda source: store)
    store.write_bytes(f"STO/documents/{key_for(PDF)}", PDF)
    store.write_bytes(f"STO/documents/{key_for(RESTRICTED)}", RESTRICTED)
    store.write_bytes(f"OLD/documents/{key_for(b'')}", b"")
    store.write_bytes(
        "STO/Minutes/json/2026-10-20-1664-cabinet.json",
        json.dumps({"documents": [{"storage_key": key_for(PDF)}]}).encode(),
    )
    store.write_bytes(
        "STO/Decisions/json/2025-03-01-12.json",
        json.dumps({"documents": [{"storage_key": key_for(RESTRICTED)}]}).encode(),
    )
    return store


def run_command(*args):
    out = io.StringIO()
    Command(["embeddings", *args], out, pretty=False).execute()
    return out.getvalue()


def dry_run(*args):
    return run_command(*args, "--dry-run")


def test_dry_run(store):
    output = dry_run("--council", "STO")
    assert output.startswith("2 files in LocalStore(")
    assert "        2  no status\n" in output
    assert "        2  extract\n" in output


def test_council_is_upper_cased(store):
    assert dry_run("--council", "sto").startswith("2 files")


def test_year(store):
    assert dry_run("--council", "STO", "--year", "2026").startswith("1 files")
    assert dry_run("--council", "STO", "--year", "2024").startswith("0 files")


def test_file(store):
    assert dry_run("--file", f"STO/{key_for(PDF)}").startswith("1 files")


def test_limit(store):
    assert dry_run("--council", "STO", "--limit", "1").startswith("1 files")


def test_a_council_or_a_file_is_required(store):
    with pytest.raises(SystemExit):
        dry_run()


def test_year_does_not_go_with_file(store):
    with pytest.raises(SystemExit):
        dry_run("--file", f"STO/{key_for(PDF)}", "--year", "2026")


def test_invalid_stages(store):
    with pytest.raises(SystemExit):
        dry_run("--council", "STO", "--stages", "extract,other")


def test_invalid_file(store):
    with pytest.raises(SystemExit):
        dry_run("--file", "STO/report.pdf")


def test_unknown_embedder(store):
    with pytest.raises(SystemExit):
        dry_run("--council", "STO", "--embedder", "other")


@pytest.fixture
def fake_converter(monkeypatch):
    """
    extract.convert() returning the fixture document, in threads rather
    than worker processes so that the patch applies to the work.
    """
    docling_core = pytest.importorskip("docling_core.types.doc")
    from pathlib import Path

    document = docling_core.DoclingDocument.load_from_json(
        Path(__file__).parent / "fixtures" / "MAN" / "decision-notice.docling.json"
    )
    monkeypatch.setattr(
        extract,
        "convert",
        lambda stream: extract.Conversion(ExtractStatus.OK, 2, None, document),
    )
    monkeypatch.setattr(extract, "docling_version", lambda: "2.130.0")
    monkeypatch.setattr(extract, "converter_device", lambda: "cpu")
    monkeypatch.setattr(extract, "pdf_title", lambda data: None)
    for module in (commands, queue_worker):
        monkeypatch.setattr(
            module,
            "worker_pool",
            lambda workers, settings: ThreadPoolExecutor(max_workers=2),
        )


def test_run(store, fake_converter):
    output = run_command("--council", "STO")
    assert f"STO {key_for(PDF)}: pdf ok 2 pages" in output
    assert "4 chunks; fake 3 embedded, 0 reused" in output
    assert f"STO {key_for(RESTRICTED)}: restricted restricted" in output
    assert "        1  ok\n" in output
    assert "        1  restricted\n" in output
    assert "2 files worked on" in output

    status = read_status(
        store, f"STO/documents/{key_for(PDF)}", f"sha256:{key_for(PDF)[:64]}"
    )
    assert status.outcome == "ok"
    assert status.embed["fake"].embedded == 3

    again = run_command("--council", "STO")
    assert "        2  ok" in again or "        1  ok" in again
    assert "        2  nothing\n" in again
    assert "0 files worked on" in again


def test_status_is_read_for_the_dry_run(store, fake_converter):
    run_command("--council", "STO")
    output = dry_run("--council", "STO")
    assert "        1  ok\n" in output
    assert "        1  restricted\n" in output
    assert "        2  nothing\n" in output


# The AWS pipeline: --queue and --drain-queue


@pytest.fixture
def aws_clients(monkeypatch):
    """Fake SQS and SNS in place of the AWS clients, and their variables."""
    from lgsf.embeddings import aws
    from lgsf.embeddings.tests.test_aws import FakeSNS, FakeSQS

    fakes = {"sqs": FakeSQS(), "sns": FakeSNS()}
    monkeypatch.setattr(aws, "get_client", fakes.__getitem__)
    monkeypatch.setenv(aws.DOCUMENTS_QUEUE_URL, "https://sqs/documents")
    monkeypatch.setenv(aws.EXTRACT_QUEUE_URL, "https://sqs/extract")
    monkeypatch.setenv(aws.EMBED_QUEUE_URL, "https://sqs/embed")
    monkeypatch.setenv(aws.FILE_READY_TOPIC_ARN, "arn:aws:sns:eu-west-2:1:ready")
    monkeypatch.setenv(aws.EMBEDDER, "fake")
    return fakes


def test_queue_sends_the_files_that_need_something(store, aws_clients):
    output = run_command("--council", "STO", "--queue")

    assert "2 files to queue" in output
    assert "2 files queued" in output
    sqs = aws_clients["sqs"]
    assert sorted(m["storage_key"] for m in sqs.bodies("https://sqs/documents")) == (
        sorted([key_for(PDF), key_for(RESTRICTED)])
    )
    assert sqs.bodies("https://sqs/extract") == []


def test_queue_skips_files_that_are_done(store, aws_clients, fake_converter):
    run_command("--council", "STO")
    output = run_command("--council", "STO", "--queue")
    assert "0 files to queue" in output
    assert aws_clients["sqs"].bodies("https://sqs/documents") == []


def test_queue_skips_files_waiting_on_the_extract_queue(store, aws_clients):
    import os
    import time

    from lgsf.embeddings import aws
    from lgsf.embeddings.files import File

    pdf = File("STO", key_for(PDF))
    restricted = File("STO", key_for(RESTRICTED))
    aws.mark_queued(store, pdf)
    aws.mark_queued(store, restricted)
    stale = time.time() - 8 * 24 * 3600
    os.utime(
        store.path(f"{restricted.document_key}.{aws.QUEUED_SUFFIX}"), (stale, stale)
    )

    output = run_command("--council", "STO", "--queue")

    assert "1 files already queued for extraction" in output
    assert "1 files to queue" in output
    assert aws_clients["sqs"].bodies("https://sqs/documents") == [
        aws.message(restricted)
    ]


def test_queue_dry_run(store, aws_clients):
    output = run_command("--council", "STO", "--queue", "--dry-run")
    assert "2 files to queue" in output
    assert "queued" not in output
    assert aws_clients["sqs"].bodies("https://sqs/documents") == []


def test_queue_refuses_force_and_retry_failed(store, aws_clients):
    with pytest.raises(SystemExit):
        run_command("--council", "STO", "--queue", "--force")
    with pytest.raises(SystemExit):
        run_command("--council", "STO", "--queue", "--retry-failed")


def test_queue_plans_for_the_lambdas_embedder(store, aws_clients, monkeypatch):
    """
    The default embedder for a run is the fake one; for --queue it's the
    one the Lambdas use, so a file embedded there counts as done.
    """
    from lgsf.embeddings.tests.test_aws import add_chunked

    monkeypatch.setenv("EMBEDDER", "titan-v2")
    add_chunked(store, b"%PDF-1.7 chunked", ["one two three"], embedder="titan-v2")
    output = run_command("--council", "STO", "--queue", "--dry-run")
    assert "3 files in" in output
    assert "2 files to queue" in output


def test_drain_queue(store, aws_clients, fake_converter):
    sqs = aws_clients["sqs"]
    sqs.send_message_batch(
        "https://sqs/extract",
        [
            {
                "Id": str(index),
                "MessageBody": json.dumps({"council": "STO", "storage_key": key}),
            }
            for index, key in enumerate([key_for(PDF), key_for(RESTRICTED)])
        ],
    )

    output = run_command("--drain-queue", "https://sqs/extract", "--workers", "1")

    assert "2 messages received, 2 files" in output
    assert f"] STO {key_for(PDF)}: pdf ok 2 pages" in output
    assert "4 chunks" in output
    assert "fake 3 embedded" not in output
    assert f"] STO {key_for(RESTRICTED)}: restricted restricted" in output
    assert "The queue is empty: stopping." in output
    assert "2 files worked on" in output
    assert [m["storage_key"] for m in sqs.bodies("https://sqs/embed")] == [key_for(PDF)]
    assert [m["outcome"] for m in aws_clients["sns"].messages] == ["restricted"]
    assert sqs.in_flight == {}


def test_drain_queue_does_not_go_with_a_council(store, aws_clients):
    with pytest.raises(SystemExit):
        run_command("--drain-queue", "https://sqs/extract", "--council", "STO")
