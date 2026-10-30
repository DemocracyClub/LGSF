"""
A run outside AWS is reported the way an AWS run is.

The dashboard reads run reports from S3, and AWS writes one per Step
Functions run. A local run writes each council's result to a file as the
worker returns it, then publishes the file as one report through the same
code the aggregation Lambda uses.
"""

import datetime
import json
from types import SimpleNamespace

import pytest

from lgsf import run_reports
from lgsf.aws_lambda import handlers
from lgsf.storage.tests.test_s3_storage import FakeS3


class ReportsS3(FakeS3):
    def put_object(self, Bucket, Key, Body, **kwargs):
        # boto3 takes a str body too, as save_to_s3 sends
        body = Body.encode() if isinstance(Body, str) else Body
        return super().put_object(Bucket=Bucket, Key=Key, Body=body, **kwargs)


@pytest.fixture
def s3(monkeypatch):
    fake = ReportsS3()
    monkeypatch.setattr(run_reports.boto3, "client", lambda name: fake)
    monkeypatch.setattr(run_reports, "cleanup_old_reports", lambda *a, **k: None)
    monkeypatch.setenv("S3_REPORTS_BUCKET", "reports")
    return fake


def run_log(error=""):
    start = datetime.datetime(2026, 10, 30, 8, 0, tzinfo=datetime.UTC)
    end = start + datetime.timedelta(seconds=12)
    return SimpleNamespace(start=start, end=end, duration=end - start, error=error)


def reports(fake):
    return [
        json.loads(o["Body"])
        for k, o in fake.objects.items()
        if k.startswith("run-reports/")
    ]


def test_a_failure_keeps_its_full_log_beside_the_report(s3):
    result = run_reports.failed_result(
        "EAT",
        "councillors",
        run_log("Traceback ...\nValueError: x"),
        ValueError("403 from the site\nmore detail"),
        bucket="reports",
    )

    assert result["error"] == "ValueError: 403 from the site"
    assert result["status"] == "failed"
    saved = json.loads(s3.objects[result["runlog_s3_key"]]["Body"])
    assert saved["error"] == "Traceback ...\nValueError: x"


def test_publish_reports_a_runs_results_as_one_run(s3, tmp_path):
    results = tmp_path / "results.jsonl"
    results.write_text(
        json.dumps(
            run_reports.completed_result("EAT", "councillors", run_log(), "r1", "abc")
        )
        + "\n"
        + json.dumps(
            run_reports.completed_result("NEC", "minutes", run_log(), "r2", None)
        )
        + "\n"
    )

    code = run_reports.main(["publish", str(results), "--execution-id", "local-box-1"])

    assert code == 0
    (report,) = reports(s3)
    assert report["execution_id"] == "local-box-1"
    assert [
        (s["council"], s["scraper_type"], s["commit"]) for s in report["scrapers"]
    ] == [
        ("EAT", "councillors", "abc"),
        ("NEC", "minutes", None),
    ]


def test_publish_fails_when_a_council_did(s3, tmp_path):
    """So whatever scheduled the run hears about it."""
    results = tmp_path / "results.jsonl"
    results.write_text(
        json.dumps(
            run_reports.failed_result("EAT", "councillors", run_log(), ValueError("no"))
        )
        + "\n"
    )

    assert run_reports.main(["publish", str(results), "--execution-id", "x"]) == 1
    assert reports(s3)[0]["summary"]["total_failed"] == 1


def test_nothing_run_means_no_report(s3, tmp_path):
    """A report takes one of the few the bucket keeps."""
    code = run_reports.main(
        ["publish", str(tmp_path / "never-written.jsonl"), "--execution-id", "x"]
    )

    assert code == 0
    assert reports(s3) == []


def test_aws_and_local_runs_report_the_same_way(s3, tmp_path):
    """The same results make the same report, whichever way they're published."""
    results = [
        run_reports.completed_result("EAT", "councillors", run_log(), "r1", "abc"),
        run_reports.failed_result("NEC", "minutes", run_log(), ValueError("no")),
    ]

    handlers.aggregation_handler(
        {"execution_id": "aws-run", "scraper_results": [results]}, None
    )
    path = tmp_path / "results.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in results))
    run_reports.main(["publish", str(path), "--execution-id", "local-run"])

    from_aws, from_local = reports(s3)
    for report in (from_aws, from_local):
        del report["timestamp"], report["execution_id"]
    assert from_aws == from_local
