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
