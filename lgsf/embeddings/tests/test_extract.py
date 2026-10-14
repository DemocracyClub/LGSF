"""
Choosing, skipping and logging the PDFs `manage.py embeddings extract`
converts.

Docling isn't loaded: _convert() and _save() are replaced with fakes, and
files are "converted" in threads rather than worker processes. The
decisions and documents are made up in a temporary directory.
"""

import datetime
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from lgsf.embeddings import extract
from lgsf.embeddings.chunkers import DecisionChunker
from lgsf.embeddings.extract import (
    ExtractionLog,
    extract_file,
    extracted_path,
    needs_extracting,
    plan_council,
    remove_temp_files,
    run_tasks,
)

PDF = b"%PDF-1.7\n"
RESTRICTED = b"<html><body>Reasons restricted</body></html>"
VERSION = "2.130.0"


def add_decision(data_path, date, identifier, documents):
    """Write a decision linking to documents, given as (storage key, hash)."""
    json_dir = data_path / "Decisions" / "json"
    json_dir.mkdir(parents=True, exist_ok=True)
    decision = {
        "raw_identifier": identifier,
        "raw_date": date,
        "raw_title": f"Decision {identifier}",
        "documents": [
            {
                "storage_key": key,
                "content_hash": f"sha256:{content_hash}",
                "title": f"Document {key}",
                "url": f"https://example.org/{key}",
            }
            for key, content_hash in documents
        ],
    }
    (json_dir / f"{date}-{identifier}.json").write_text(json.dumps(decision))


def add_file(data_path, key, content=PDF):
    documents_dir = data_path / "Decisions" / "documents"
    documents_dir.mkdir(parents=True, exist_ok=True)
    (documents_dir / key).write_bytes(content)


@pytest.fixture
def data_path(tmp_path):
    """
    Three decisions in MAN. The report "aaa" is linked from the first
    two, under a different key each time.
    """
    path = tmp_path / "MAN"
    add_decision(path, "2026-02-01", "1", [("2026-02-01-1-s1.pdf", "aaa")])
    add_decision(
        path,
        "2026-03-01",
        "2",
        [("2026-03-01-2-s5.pdf", "aaa"), ("2026-03-01-2-s6.pdf", "bbb")],
    )
    add_decision(path, "2026-04-01", "3", [("2026-04-01-3-s7.pdf", "ccc")])
    for key in ("2026-02-01-1-s1.pdf", "2026-03-01-2-s5.pdf", "2026-03-01-2-s6.pdf"):
        add_file(path, key)
    add_file(path, "2026-04-01-3-s7.pdf", RESTRICTED)
    return path


def plan(data_path, **options):
    documents = DecisionChunker("MAN", data_path=data_path).linked_documents()
    log = ExtractionLog(extracted_path(data_path))
    return plan_council("MAN", documents, log, VERSION, **options), log


class FakeDocument:
    def __init__(self, text):
        self.text = text


@pytest.fixture
def fake_docling(monkeypatch):
    """
    Pretend to convert: each PDF gives its own name as its text, except
    any named in `results`, which give the (status, pages, error) set
    there.
    """
    results = {}

    def convert(source):
        status, pages, error = results.get(source.name, ("ok", 2, None))
        document = FakeDocument(source.name) if status in ("ok", "partial") else None
        return status, pages, error, document

    def save(document, path):
        path.write_text(json.dumps({"text": document.text}))

    monkeypatch.setattr(extract, "_convert", convert)
    monkeypatch.setattr(extract, "_save", save)
    return results


def run(tasks):
    with ThreadPoolExecutor(max_workers=2) as executor:
        run_tasks(tasks, executor, VERSION)


def log_lines(log):
    return [json.loads(line) for line in log.path.read_text().splitlines()]


def test_a_file_linked_under_several_keys_is_extracted_once(data_path):
    result, _ = plan(data_path)
    keys = sorted(task.document.storage_key for task in result.tasks)
    # "aaa" is named after the key of the earliest decision linking to it.
    assert keys == ["2026-02-01-1-s1.pdf", "2026-03-01-2-s6.pdf"]
    aaa = next(t for t in result.tasks if t.document.content_hash == "sha256:aaa")
    assert aaa.target.name == "2026-02-01-1-s1.json"
    assert [d["raw_identifier"] for d in aaa.document.decisions] == ["1", "2"]


def test_files_that_arent_pdfs_are_logged_and_not_converted(data_path):
    result, log = plan(data_path)
    assert result.not_pdf == 1
    assert "sha256:ccc" not in {t.document.content_hash for t in result.tasks}
    assert log_lines(log) == [
        {
            "content_hash": "sha256:ccc",
            "storage_key": "2026-04-01-3-s7.pdf",
            "file": None,
            "status": "not_pdf",
            "pages": None,
            "seconds": 0,
            "error": None,
            "docling_version": VERSION,
        }
    ]
    # And aren't looked at again.
    result, _ = plan(data_path)
    assert result.not_pdf == 0
    assert result.done == 1


def test_files_not_on_disk_are_counted_but_not_logged(data_path):
    (data_path / "Decisions" / "documents" / "2026-03-01-2-s6.pdf").unlink()
    result, log = plan(data_path)
    assert result.missing == 1
    assert "sha256:bbb" not in {entry["content_hash"] for entry in log_lines(log)}


def test_date_range(data_path):
    documents = DecisionChunker(
        "MAN", data_path=data_path, since=datetime.date(2026, 3, 1)
    ).linked_documents()
    # Linked from decision 2 now, so named after its key.
    assert documents["sha256:aaa"].storage_key == "2026-03-01-2-s5.pdf"


def test_results_are_logged_and_saved(data_path, fake_docling):
    fake_docling["2026-03-01-2-s6.pdf"] = ("failed", 3, "Broken PDF")
    result, log = plan(data_path)
    run(result.tasks)

    entries = log.read()
    assert entries["sha256:aaa"]["status"] == "ok"
    assert entries["sha256:aaa"]["file"] == "2026-02-01-1-s1.json"
    assert entries["sha256:aaa"]["pages"] == 2
    assert entries["sha256:aaa"]["docling_version"] == VERSION
    assert entries["sha256:bbb"]["status"] == "failed"
    assert entries["sha256:bbb"]["error"] == "Broken PDF"
    assert entries["sha256:bbb"]["file"] is None

    assert log.extracted_file(entries["sha256:aaa"]).read_text() == (
        '{"text": "2026-02-01-1-s1.pdf"}'
    )
    assert log.extracted_file(entries["sha256:bbb"]) is None
    assert sorted(p.name for p in log.directory.iterdir()) == [
        "2026-02-01-1-s1.json",
        "log.jsonl",
    ]


def test_a_run_skips_what_is_done(data_path, fake_docling):
    fake_docling["2026-03-01-2-s6.pdf"] = ("timeout", 300, "Timed out")
    result, _ = plan(data_path)
    run(result.tasks)

    result, _ = plan(data_path)
    assert result.tasks == []
    assert result.done == 3


def test_retry_failed(data_path, fake_docling):
    fake_docling["2026-03-01-2-s6.pdf"] = ("timeout", 300, "Timed out")
    result, _ = plan(data_path)
    run(result.tasks)

    result, _ = plan(data_path, retry_failed=True)
    assert [t.document.content_hash for t in result.tasks] == ["sha256:bbb"]


def test_force(data_path, fake_docling):
    result, _ = plan(data_path)
    run(result.tasks)
    result, _ = plan(data_path, force=True)
    assert len(result.tasks) == 2


def test_the_last_line_for_a_file_counts(data_path, fake_docling):
    fake_docling["2026-03-01-2-s6.pdf"] = ("failed", 3, "Broken PDF")
    result, log = plan(data_path)
    run(result.tasks)
    del fake_docling["2026-03-01-2-s6.pdf"]
    result, _ = plan(data_path, retry_failed=True)
    run(result.tasks)

    statuses = [
        e["status"] for e in log_lines(log) if e["content_hash"] == "sha256:bbb"
    ]
    assert statuses == ["failed", "ok"]
    assert log.read()["sha256:bbb"]["status"] == "ok"
    result, _ = plan(data_path, retry_failed=True)
    assert result.tasks == []


def test_a_json_file_with_no_log_line_is_extracted_again(data_path, fake_docling):
    # As left by a run stopped after the file was renamed but before its
    # line was logged.
    directory = extracted_path(data_path)
    directory.mkdir(parents=True)
    (directory / "2026-02-01-1-s1.json").write_text("{}")
    result, _ = plan(data_path)
    assert "sha256:aaa" in {t.document.content_hash for t in result.tasks}


def test_temp_files_are_removed(data_path):
    directory = extracted_path(data_path)
    directory.mkdir(parents=True)
    (directory / "2026-02-01-1-s1.json.tmp").write_text("{")
    (directory / "2026-03-01-2-s6.json").write_text("{}")
    assert remove_temp_files(ExtractionLog(directory)) == 1
    assert [p.name for p in directory.iterdir()] == ["2026-03-01-2-s6.json"]


def test_a_line_cut_off_by_a_stopped_run_is_ignored(tmp_path):
    log = ExtractionLog(tmp_path)
    log.append({"content_hash": "sha256:aaa", "status": "ok"})
    with log.path.open("a") as f:
        f.write('{"content_hash": "sha256:bbb", "sta')
    log.append({"content_hash": "sha256:ccc", "status": "ok"})
    assert sorted(log.read()) == ["sha256:aaa", "sha256:ccc"]


def test_needs_extracting():
    assert needs_extracting(None)
    for status in ("ok", "partial", "not_pdf"):
        assert not needs_extracting({"status": status})
        assert not needs_extracting({"status": status}, retry_failed=True)
        assert needs_extracting({"status": status}, force=True)
    for status in ("failed", "timeout"):
        assert not needs_extracting({"status": status})
        assert needs_extracting({"status": status}, retry_failed=True)


def test_extract_file_saves_through_a_temp_file(tmp_path, monkeypatch):
    target = tmp_path / "out.json"
    saved_to = []

    def save(document, path):
        saved_to.append(path.name)
        path.write_text("{}")

    monkeypatch.setattr(extract, "_convert", lambda s: ("ok", 1, None, object()))
    monkeypatch.setattr(extract, "_save", save)
    result = extract_file(tmp_path / "in.pdf", target)
    assert saved_to == ["out.json.tmp"]
    assert [p.name for p in tmp_path.iterdir()] == ["out.json"]
    assert result["status"] == "ok"
    assert result["file"] == "out.json"


def test_extract_file_makes_the_extracted_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "_convert", lambda s: ("ok", 1, None, object()))
    monkeypatch.setattr(extract, "_save", lambda document, path: path.write_text("{}"))
    target = tmp_path / "Embeddings" / "extracted" / "out.json"
    assert extract_file(tmp_path / "in.pdf", target)["status"] == "ok"
    assert target.exists()


def test_extract_file_reports_an_exception_as_failed(tmp_path, monkeypatch):
    def convert(source):
        raise ValueError("Bad page tree")

    monkeypatch.setattr(extract, "_convert", convert)
    result = extract_file(tmp_path / "in.pdf", tmp_path / "out.json")
    assert result["status"] == "failed"
    assert result["error"] == "ValueError: Bad page tree"
    assert result["file"] is None
    assert list(tmp_path.iterdir()) == []


def test_smallest_files_go_first(data_path, monkeypatch):
    pages = {"2026-02-01-1-s1.pdf": 300, "2026-03-01-2-s6.pdf": 4}
    monkeypatch.setattr(extract, "count_pages", lambda path: pages[path.name])
    result, _ = plan(data_path)
    ordered = sorted(result.tasks, key=extract.ExtractionTask.sort_key)
    assert [t.pages for t in ordered] == [4, 300]
