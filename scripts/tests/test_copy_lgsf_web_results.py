"""
Copying lgsf-web's Docling results into the pipeline's layout, against
the fake S3 client the storage tests use.
"""

import hashlib
import json
from pathlib import Path

import jsonschema
import pytest

from lgsf.embeddings.sidecars import FileStatus
from lgsf.storage import s3
from lgsf.storage.tests.test_s3_storage import FakeS3
from scripts.copy_lgsf_web_results import Copier, main

SCHEMA = (
    Path(__file__).parent.parent.parent
    / "lgsf"
    / "embeddings"
    / "schemas"
    / "v1"
    / "status.schema.json"
)

REPORT = b"%PDF-1.7 a report"
APPENDIX = b"%PDF-1.7 an appendix"
RESTRICTED_PAGE = b"<html><h1>Reasons restricted</h1></html>"
LOGON_PAGE = b"<html><body>Logon</body></html>"
BROKEN = b"%PDF-1.7 broken"
LEFT_BEHIND = b"%PDF-1.7 never migrated"

OLD_DIR = "data/PEM/Decisions/documents/"
NEW_DIR = "merged-data/PEM/documents/"


def sha256(content):
    return hashlib.sha256(content).hexdigest()


def put(fake, key, body, content_type=None):
    fake.objects[key] = {
        "Body": body if isinstance(body, bytes) else body.encode(),
        "ContentType": content_type,
        "Metadata": {},
    }


def old_status(content, status, old_key, format=None, **extract):
    """What lgsf-web wrote, by the extraction's status."""
    entry = {
        "status": status,
        "pages": None,
        "seconds": 1.9,
        "error": None,
        "docling_version": None,
        "device": None,
        "finished_at": "2026-10-20T16:02:58+00:00",
    }
    entry.update(extract)
    data = {
        "content_hash": f"sha256:{sha256(content)}",
        "storage_keys": [old_key],
        "format": format
        or ("pdf" if status in ("ok", "partial", "failed", "timeout") else None),
        "extract": entry,
        "chunk": None,
        "embed": {},
    }
    if status in ("ok", "partial"):
        data["chunk"] = {"inputs_hash": "sha256:" + "a" * 64, "max_words": 350}
        data["embed"] = {"titan-v2": {"model": "amazon.titan-embed-text-v2:0"}}
    return data


def add_old(fake, old_key, content, status, results=True, new_extension=".pdf", **kw):
    """
    A document in the old layout with lgsf-web's results, and its copy
    at a content key in the new layout (unless ``new_extension`` is None).
    """
    data = old_status(content, status, old_key, **kw)
    put(fake, f"{OLD_DIR}{old_key}", content, "application/pdf")
    put(fake, f"{OLD_DIR}{old_key}.status.json", json.dumps(data))
    if results and status in ("ok", "partial"):
        put(fake, f"{OLD_DIR}{old_key}.docling.json", b'{"schema_name": "Docling"}')
        put(fake, f"{OLD_DIR}{old_key}.md", b"# A report\n")
        put(fake, f"{OLD_DIR}{old_key}.chunks.jsonl", b"{}\n")
        put(fake, f"{OLD_DIR}{old_key}.titan-v2.jsonl", b"{}\n")
    if new_extension is not None:
        put(fake, f"{NEW_DIR}{sha256(content)}{new_extension}", content)
    return f"{sha256(content)}{new_extension}" if new_extension is not None else None


@pytest.fixture
def fake():
    return FakeS3()


@pytest.fixture
def pembrokeshire(fake):
    """One of each kind of old result."""
    return {
        "ok": add_old(
            fake,
            "2026-01-12-3064-s82290.pdf",
            REPORT,
            "ok",
            pages=23,
            docling_version="2.130.0",
            device="cuda",
        ),
        "partial": add_old(
            fake,
            "2026-01-12-3065-s82291.pdf",
            APPENDIX,
            "partial",
            pages=4,
            docling_version="2.130.0",
            device="cuda",
            error="Timed out on page 4",
        ),
        "restricted": add_old(
            fake,
            "2026-01-12-3066-s82292.pdf",
            RESTRICTED_PAGE,
            "restricted",
            format="restricted",
            new_extension=".html",
        ),
        "not_document": add_old(
            fake,
            "2026-01-12-3067-s82293.pdf",
            LOGON_PAGE,
            "not_document",
            format="html",
            new_extension=".html",
        ),
        "empty": add_old(
            fake,
            "2026-01-12-3068-s82294.pdf",
            b"",
            "empty",
            format="empty",
            new_extension="",
        ),
        "failed": add_old(
            fake,
            "2026-01-12-3069-s82295.pdf",
            BROKEN,
            "failed",
            error="RuntimeError: boom",
            docling_version="2.130.0",
        ),
        "left_behind": add_old(
            fake, "2026-01-12-3070-s82296.pdf", LEFT_BEHIND, "ok", new_extension=None
        ),
    }


def copier(fake, **kwargs):
    kwargs.setdefault("workers", 2)
    kwargs.setdefault("out", lambda *a: None)
    return Copier(fake, "bucket", **kwargs)


def new_status(fake, new_key):
    return json.loads(fake.objects[f"{NEW_DIR}{new_key}.status.json"]["Body"])


def validate(data):
    schema = json.loads(SCHEMA.read_text())
    validator = jsonschema.Draft202012Validator(
        schema, format_checker=jsonschema.FormatChecker()
    )
    errors = sorted(validator.iter_errors(data), key=str)
    assert not errors, "\n".join(error.message for error in errors)


# ---- each kind ----


def test_an_extracted_file_gets_its_docling_output_and_an_extract_only_status(
    fake, pembrokeshire
):
    report = copier(fake).run_council("PEM")

    assert not report.failed
    new_key = pembrokeshire["ok"]
    docling = fake.objects[f"{NEW_DIR}{new_key}.docling-v2.json"]
    assert docling["Body"] == b'{"schema_name": "Docling"}'
    assert docling["ContentType"] == "application/json; charset=utf-8"
    markdown = fake.objects[f"{NEW_DIR}{new_key}.md"]
    assert markdown["Body"] == b"# A report\n"
    assert markdown["ContentType"] == "text/markdown; charset=utf-8"
    assert f"{NEW_DIR}{new_key}.chunks.jsonl" not in fake.objects
    assert f"{NEW_DIR}{new_key}.titan-v2.jsonl" not in fake.objects

    data = new_status(fake, new_key)
    validate(data)
    assert data["council"] == "PEM"
    assert data["storage_key"] == new_key
    assert data["content_hash"] == f"sha256:{sha256(REPORT)}"
    assert data["format"] == "pdf"
    assert data["outcome"] == "ok"
    assert data["error"] is None
    assert data["stages"] == {
        "extract": {
            "version": 2,
            "tool_version": "2.130.0",
            "finished_at": "2026-10-20T16:02:58+00:00",
            "status": "ok",
            "pages": 23,
            "seconds": 1.9,
            "device": "cuda",
        }
    }
    # The pipeline reads it as extracted and not chunked.
    status = FileStatus.from_dict(data)
    assert status.extracted and not status.chunked


def test_a_partial_extraction_is_ok_with_doclings_message(fake, pembrokeshire):
    copier(fake).run_council("PEM")

    data = new_status(fake, pembrokeshire["partial"])
    validate(data)
    assert data["outcome"] == "ok"
    assert data["error"] == "Timed out on page 4"
    assert data["stages"]["extract"]["status"] == "partial"
    assert f"{NEW_DIR}{pembrokeshire['partial']}.docling-v2.json" in fake.objects


@pytest.mark.parametrize(
    "kind, outcome, status",
    [
        ("restricted", "restricted", "restricted"),
        ("not_document", "not_a_document", "not_document"),
        ("empty", "empty", "empty"),
    ],
)
def test_a_left_out_file_gets_only_a_status(fake, pembrokeshire, kind, outcome, status):
    copier(fake).run_council("PEM")

    new_key = pembrokeshire[kind]
    data = new_status(fake, new_key)
    validate(data)
    assert data["outcome"] == outcome
    assert data["stages"]["extract"]["status"] == status
    assert data["stages"]["extract"]["tool_version"] is None
    assert "chunk" not in data["stages"] and "embed" not in data["stages"]
    assert [k for k in fake.objects if k.startswith(f"{NEW_DIR}{new_key}.")] == [
        f"{NEW_DIR}{new_key}.status.json"
    ]


def test_unsupported_is_not_a_document(fake):
    new_key = add_old(
        fake,
        "2026-01-01-1-s1.pdf",
        b"PK\x03\x04 a zip",
        "unsupported",
        format="unsupported",
        new_extension=".zip",
    )

    copier(fake).run_council("PEM")

    data = new_status(fake, new_key)
    validate(data)
    assert data["outcome"] == "not_a_document"
    assert data["stages"]["extract"]["status"] == "unsupported"


def test_a_failed_extraction_is_skipped(fake, pembrokeshire):
    report = copier(fake).run_council("PEM")

    assert report.skipped_failed == 1
    assert f"{NEW_DIR}{pembrokeshire['failed']}.status.json" not in fake.objects


def test_a_timeout_is_skipped_too(fake):
    new_key = add_old(fake, "2026-01-01-1-s1.pdf", BROKEN, "timeout")

    report = copier(fake).run_council("PEM")

    assert report.skipped_failed == 1
    assert f"{NEW_DIR}{new_key}.status.json" not in fake.objects


def test_a_file_not_in_merged_data_is_counted_and_skipped(fake, pembrokeshire):
    report = copier(fake).run_council("PEM")

    assert report.not_in_merged == 1
    assert not [k for k in fake.objects if sha256(LEFT_BEHIND) in k]


def test_the_counts(fake, pembrokeshire):
    report = copier(fake).run_council("PEM")

    assert (
        report.statuses,
        report.copied,
        report.cheap,
        report.present,
        report.not_in_merged,
        report.skipped_failed,
        report.problems,
        report.errors,
    ) == (7, 2, 3, 0, 1, 1, [], [])


# ---- running it again ----


def test_a_rerun_writes_nothing(fake, pembrokeshire):
    copier(fake).run_council("PEM")
    before = {k: dict(v) for k, v in fake.objects.items()}
    fake.calls.clear()

    report = copier(fake).run_council("PEM")

    assert fake.objects == before
    assert not [op for op, _ in fake.calls if op in ("put", "copy", "delete")]
    assert report.present == 5
    assert report.copied == 0 and report.cheap == 0


def test_a_status_already_in_the_new_layout_is_left_alone(fake, pembrokeshire):
    new_key = pembrokeshire["ok"]
    put(fake, f"{NEW_DIR}{new_key}.status.json", b'{"theirs": true}')

    report = copier(fake).run_council("PEM")

    assert report.present == 1
    assert (
        fake.objects[f"{NEW_DIR}{new_key}.status.json"]["Body"] == b'{"theirs": true}'
    )
    assert f"{NEW_DIR}{new_key}.docling-v2.json" not in fake.objects


def test_two_old_keys_for_the_same_bytes_give_one_copy(fake):
    """A document linked from two decisions had results at both keys."""
    add_old(fake, "2026-01-01-1-s1.pdf", REPORT, "ok", docling_version="2.130.0")
    new_key = add_old(
        fake, "2026-01-02-2-s2.pdf", REPORT, "ok", docling_version="2.130.0"
    )

    report = copier(fake).run_council("PEM")

    assert (report.copied, report.present) == (1, 1)
    copies = [k for op, k in fake.calls if op == "copy"]
    assert copies == [
        f"{NEW_DIR}{new_key}.docling-v2.json",
        f"{NEW_DIR}{new_key}.md",
    ]


# ---- order, safety and the dry run ----


def test_the_status_is_written_after_the_files_it_vouches_for(fake, pembrokeshire):
    copier(fake, workers=1).run_council("PEM")

    new_key = pembrokeshire["ok"]
    writes = [k for op, k in fake.calls if op in ("copy", "put") and new_key in k]
    assert writes == [
        f"{NEW_DIR}{new_key}.docling-v2.json",
        f"{NEW_DIR}{new_key}.md",
        f"{NEW_DIR}{new_key}.status.json",
    ]


def test_nothing_is_deleted_and_the_old_layout_is_untouched(fake, pembrokeshire):
    old = {k: dict(v) for k, v in fake.objects.items() if k.startswith("data/")}

    copier(fake).run_council("PEM")

    assert not [op for op, _ in fake.calls if op == "delete"]
    assert {k: v for k, v in fake.objects.items() if k.startswith("data/")} == old


def test_each_directory_is_listed_once(fake, pembrokeshire):
    copier(fake).run_council("PEM")

    assert fake.ops("list") == [OLD_DIR, NEW_DIR]
    assert fake.ops("head") == []


def test_a_dry_run_writes_nothing_and_says_what_it_would_do(fake, pembrokeshire):
    before = dict(fake.objects)
    lines = []

    report = copier(fake, dry_run=True, out=lines.append).run_council("PEM")

    assert fake.objects == before
    assert not [op for op, _ in fake.calls if op in ("put", "copy")]
    assert (
        report.copied,
        report.cheap,
        report.not_in_merged,
        report.skipped_failed,
    ) == (
        2,
        3,
        1,
        1,
    )
    kinds = {line.split(": ")[1].split(" -> ")[0]: line for line in lines}
    assert "copy (ok, 23 pages)" in kinds["2026-01-12-3064-s82290.pdf"]
    assert "cheap (restricted)" in kinds["2026-01-12-3066-s82292.pdf"]
    assert "skipped_failed (failed)" in kinds["2026-01-12-3069-s82295.pdf"]
    assert "not_in_merged" in kinds["2026-01-12-3070-s82296.pdf"]


def test_a_copy_that_fails_leaves_no_status(fake, pembrokeshire):
    new_key = pembrokeshire["ok"]
    fake.fail_on = f"{new_key}.md"

    report = copier(fake).run_council("PEM")

    assert report.failed
    assert len(report.errors) == 1 and "2026-01-12-3064-s82290.pdf" in report.errors[0]
    assert f"{NEW_DIR}{new_key}.status.json" not in fake.objects
    # The others were done.
    assert f"{NEW_DIR}{pembrokeshire['partial']}.status.json" in fake.objects
    assert report.copied == 1


def test_an_old_status_without_its_docling_output_is_a_problem(fake):
    new_key = add_old(fake, "2026-01-01-1-s1.pdf", REPORT, "ok", results=False)

    report = copier(fake).run_council("PEM")

    assert report.problems == ["2026-01-01-1-s1.pdf: no .docling.json or .md to copy"]
    assert f"{NEW_DIR}{new_key}.status.json" not in fake.objects
    assert not report.failed


def test_an_unreadable_old_status_is_a_problem(fake):
    put(fake, f"{OLD_DIR}2026-01-01-1-s1.pdf.status.json", b"not json")

    report = copier(fake).run_council("PEM")

    assert len(report.problems) == 1 and "unreadable" in report.problems[0]
    assert not report.failed


def test_a_council_with_nothing_in_the_old_layout(fake):
    put(fake, f"{NEW_DIR}{'a' * 64}.pdf", REPORT)

    report = copier(fake).run_council("PEM")

    assert report.statuses == 0 and not report.failed


# ---- the command line ----


@pytest.fixture
def environment(fake, monkeypatch):
    monkeypatch.setattr(s3, "get_client", lambda: fake)
    monkeypatch.setenv("LGSF_S3_BUCKET", "bucket")
    monkeypatch.delenv("LGSF_S3_PREFIX", raising=False)


def test_the_councils_are_those_with_old_results(fake, pembrokeshire):
    put(fake, "data/FLN/Decisions/documents/2026-01-01-1-s1.pdf", REPORT)
    put(fake, "data/KIR/Councillors/json/a.json", b"{}")
    put(fake, "merged-data/SWA/documents/" + "b" * 64 + ".pdf", REPORT)

    assert copier(fake).councils() == ["PEM"]


def test_the_command_line(fake, pembrokeshire, environment, capsys):
    assert main(["--council", "pem"]) == 0

    out = capsys.readouterr().out
    assert "for 1 councils" in out
    assert "[1/1] PEM: 7 statuses, 2 copied, 3 cheap" in out
    assert "Total" in out
    assert f"{NEW_DIR}{pembrokeshire['ok']}.status.json" in fake.objects


def test_the_command_line_fails_when_the_store_did(
    fake, pembrokeshire, environment, capsys
):
    fake.fail_on = f"{pembrokeshire['ok']}.md"

    assert main([]) == 1
    assert "error: 2026-01-12-3064-s82290.pdf" in capsys.readouterr().out


def test_the_command_line_dry_run(fake, pembrokeshire, environment, capsys):
    before = dict(fake.objects)

    assert main(["--dry-run"]) == 0

    assert fake.objects == before
    out = capsys.readouterr().out
    assert "(dry run)" in out
    assert "Would copy" in out
