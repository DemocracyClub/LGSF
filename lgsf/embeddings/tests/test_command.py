"""
``manage.py embeddings``: choosing files, --dry-run, and a run against
made-up files with a fake converter.
"""

import hashlib
import io
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from lgsf.embeddings import commands, extract
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
    monkeypatch.setattr(
        commands,
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
