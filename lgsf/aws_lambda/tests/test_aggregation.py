"""
Aggregation reads the Map's results, one list of per-type results per
council, and names the report by the Step Functions execution.
"""

from types import SimpleNamespace

from lgsf.aws_lambda import handlers


def run(council, scraper_type, status="completed", **extra):
    return {
        "council": council,
        "scraper_type": scraper_type,
        "status": status,
        "statusCode": 200,
        **extra,
    }


def test_nested_per_council_results_are_flattened():
    results = [
        [
            run("ABC", "councillors"),
            run("ABC", "minutes", status="failed", statusCode=500, error="E"),
        ],
        [run("KIR", "councillors")],
    ]

    assert [
        (r["council"], r["scraper_type"])
        for r in handlers.flatten_scraper_results(results)
    ] == [
        ("ABC", "councillors"),
        ("ABC", "minutes"),
        ("KIR", "councillors"),
    ]


def test_old_shapes_still_flatten():
    """One dict per council, or a Lambda result still wrapped in Payload."""
    results = [
        run("ABC", "councillors"),
        {"Payload": run("KIR", "councillors")},
        [{"Payload": run("STO", "councillors")}],
    ]

    assert [r["council"] for r in handlers.flatten_scraper_results(results)] == [
        "ABC",
        "KIR",
        "STO",
    ]


def test_aggregation_counts_every_type_and_names_the_execution(monkeypatch):
    monkeypatch.delenv("S3_REPORTS_BUCKET", raising=False)
    event = {
        "execution_id": "scraper-all-20261024-060000",
        "scraper_results": [
            [
                run("ABC", "councillors"),
                run("ABC", "minutes", status="failed", statusCode=500, error="boom"),
            ],
            [run("KIR", "councillors"), run("KIR", "minutes", status="disabled")],
        ],
    }

    result = handlers.aggregation_handler(
        event, SimpleNamespace(aws_request_id="lambda-request-id")
    )

    assert result["summary"]["total_ran"] == 4
    assert (
        result["summary"]["total_succeeded"],
        result["summary"]["total_failed"],
        result["summary"]["total_disabled"],
    ) == (2, 1, 1)
    assert result["failed_scrapers"] == [
        {"council": "ABC", "scraper_type": "minutes", "error": "boom"}
    ]


def test_the_report_records_the_execution_name(monkeypatch):
    saved = {}
    monkeypatch.setenv("S3_REPORTS_BUCKET", "bucket")
    monkeypatch.setattr(
        handlers,
        "save_to_s3",
        lambda bucket, key, data, console: saved.update(key=key, data=data) or True,
    )
    monkeypatch.setattr(handlers, "cleanup_old_reports", lambda *a, **k: None)

    handlers.aggregation_handler(
        {
            "execution_id": "abc12345-daily",
            "scraper_results": [[run("ABC", "councillors")]],
        },
        SimpleNamespace(aws_request_id="lambda-request-id"),
    )

    assert saved["data"]["execution_id"] == "abc12345-daily"
    assert saved["key"].endswith("-abc12345.json")


def test_without_an_execution_name_the_request_id_is_used_as_before(monkeypatch):
    saved = {}
    monkeypatch.setenv("S3_REPORTS_BUCKET", "bucket")
    monkeypatch.setattr(
        handlers,
        "save_to_s3",
        lambda bucket, key, data, console: saved.update(data=data) or True,
    )
    monkeypatch.setattr(handlers, "cleanup_old_reports", lambda *a, **k: None)

    handlers.aggregation_handler(
        {"scraper_results": []}, SimpleNamespace(aws_request_id="req-1")
    )

    assert saved["data"]["execution_id"] == "req-1"


# --- results written to S3 by the Map ---

import json  # noqa: E402

import pytest  # noqa: E402

from lgsf.storage.tests.test_s3_storage import FakeS3  # noqa: E402


@pytest.fixture
def written_map_results(monkeypatch):
    """What the Map's result writer leaves in the reports bucket, FLATTENed."""

    class ReportsS3(FakeS3):
        def put_object(self, Bucket, Key, Body, **kwargs):
            # boto3 takes a str body too, as save_to_s3 sends
            body = Body.encode() if isinstance(Body, str) else Body
            return super().put_object(Bucket=Bucket, Key=Key, Body=body, **kwargs)

    fake = ReportsS3()
    monkeypatch.setattr(handlers.boto3, "client", lambda name: fake)
    prefix = "map-results/abc-run"

    def put(key, value):
        fake.put_object(Bucket="reports", Key=key, Body=json.dumps(value).encode())

    put(
        f"{prefix}/SUCCEEDED_0.json", [run("ABC", "councillors"), run("ABC", "minutes")]
    )
    put(f"{prefix}/SUCCEEDED_1.json", [run("KIR", "councillors")])
    put(
        f"{prefix}/FAILED_0.json",
        [{"Error": "States.Timeout", "Cause": "child timed out"}],
    )
    put(
        f"{prefix}/manifest.json",
        {
            "DestinationBucket": "reports",
            "MapRunArn": "arn:aws:states:eu-west-2:1:mapRun:x",
            "ResultFiles": {
                "SUCCEEDED": [
                    {"Key": f"{prefix}/SUCCEEDED_0.json", "Size": 1},
                    {"Key": f"{prefix}/SUCCEEDED_1.json", "Size": 1},
                ],
                "FAILED": [{"Key": f"{prefix}/FAILED_0.json", "Size": 1}],
                "PENDING": [],
            },
        },
    )
    pointer = {
        "MapRunArn": "arn:aws:states:eu-west-2:1:mapRun:x",
        "ResultWriterDetails": {"Bucket": "reports", "Key": f"{prefix}/manifest.json"},
    }
    return fake, pointer


def test_results_are_read_from_the_manifest_the_map_wrote(written_map_results):
    fake, pointer = written_map_results

    results, keys = handlers.load_map_results(pointer)

    assert [(r["council"], r["scraper_type"], r["status"]) for r in results] == [
        ("ABC", "councillors", "completed"),
        ("ABC", "minutes", "completed"),
        ("KIR", "councillors", "completed"),
        ("unknown", "unknown", "failed"),
    ]
    assert results[-1]["error"] == "States.Timeout"
    assert len(keys) == 4


def test_a_plain_list_needs_no_s3():
    results = [[run("ABC", "councillors")]]
    assert handlers.load_map_results(results) == (results, [])


def test_aggregation_reports_from_s3_then_removes_the_map_files(
    written_map_results, monkeypatch
):
    fake, pointer = written_map_results
    monkeypatch.setenv("S3_REPORTS_BUCKET", "reports")
    monkeypatch.setattr(handlers, "cleanup_old_reports", lambda *a, **k: None)

    result = handlers.aggregation_handler(
        {"execution_id": "daily-1", "scraper_results": pointer},
        SimpleNamespace(aws_request_id="req"),
    )

    assert result["summary"]["total_ran"] == 4
    assert result["summary"]["total_failed"] == 1
    report = json.loads(fake.objects[result["report_s3_key"]]["Body"])
    assert len(report["scrapers"]) == 4
    assert not [k for k in fake.objects if k.startswith("map-results/")]
