"""
Run reports: what a run did, council by council, as the dashboard reads them.

A run's report lives at run-reports/ in the reports bucket, and only the
last REPORTS_KEPT are kept; lgsf-dashboard builds from those. A failed
scraper's full log goes beside it under runlogs/, and its entry points
there.

Step Functions runs write theirs from the aggregation Lambda, out of the
results its scraper workers returned. A run anywhere else writes the same
kind: each `manage.py <type> --results-file FILE` adds its councils'
results to FILE, in the worker's shape, and

    python -m lgsf.run_reports publish FILE --execution-id ID

reports them as one run. One report for the whole run, however many
commands it took, because each report takes one of the REPORTS_KEPT.
"""

import argparse
import datetime
import json
import os
import sys
import traceback

import boto3
from rich.console import Console

#: How many reports the bucket keeps, and so the dashboard can read. It
#: shows ten days, and a day can have several runs: the scheduled one,
#: runs on request, and runs of scrapers kept off AWS.
REPORTS_KEPT = 40


def save_to_s3(bucket_name: str, key: str, data: dict, console: Console) -> bool:
    """Save data as JSON to S3. True if it was saved."""
    try:
        s3_client = boto3.client("s3")
        json_data = json.dumps(data, indent=2, default=str)

        s3_client.put_object(
            Bucket=bucket_name, Key=key, Body=json_data, ContentType="application/json"
        )

        console.log(f"Successfully saved to S3: s3://{bucket_name}/{key}")
        return True
    except Exception as e:
        console.log(f"Error saving to S3: {e}")
        console.log(traceback.format_exc())
        return False


def cleanup_old_reports(
    bucket_name: str, console: Console, keep_count: int = REPORTS_KEPT
) -> None:
    """
    Clean up old run reports and associated RunLogs, keeping only the most recent ones.

    Args:
        bucket_name: S3 bucket name
        console: Rich console for logging
        keep_count: Number of recent reports to keep (default: REPORTS_KEPT)
    """
    try:
        s3_client = boto3.client("s3")

        # List all run reports
        console.log(f"Listing run reports in s3://{bucket_name}/run-reports/")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="run-reports/")

        if "Contents" not in response:
            console.log("No run reports found to clean up")
            return

        # Sort by last modified date (newest first)
        reports = sorted(
            response["Contents"], key=lambda x: x["LastModified"], reverse=True
        )

        # Skip the most recent N reports
        reports_to_delete = reports[keep_count:]

        if not reports_to_delete:
            console.log(f"Only {len(reports)} reports found, no cleanup needed")
            return

        console.log(
            f"Found {len(reports)} reports, keeping {keep_count}, deleting {len(reports_to_delete)}"
        )

        # Extract timestamps from report filenames to find associated RunLogs
        # Format: run-report-{timestamp}-{execution_id}.json
        timestamps_to_delete = set()
        for report in reports_to_delete:
            key = report["Key"]
            filename = key.split("/")[-1]  # Get filename from key
            # Extract timestamp (format: run-report-YYYYMMDD-HHMMSS-{id}.json)
            parts = filename.replace("run-report-", "").split("-")
            if len(parts) >= 2:
                timestamp = f"{parts[0]}-{parts[1]}"  # YYYYMMDD-HHMMSS
                timestamps_to_delete.add(timestamp)

        # Delete old run reports
        for report in reports_to_delete:
            try:
                s3_client.delete_object(Bucket=bucket_name, Key=report["Key"])
                console.log(f"Deleted old report: {report['Key']}")
            except Exception as e:
                console.log(f"Error deleting {report['Key']}: {e}")

        # Delete associated RunLog directories
        for timestamp in timestamps_to_delete:
            runlog_prefix = f"runlogs/{timestamp}/"

            try:
                # List all objects in this timestamp directory
                runlog_response = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=runlog_prefix
                )

                if "Contents" in runlog_response:
                    for obj in runlog_response["Contents"]:
                        s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
                        console.log(f"Deleted old RunLog: {obj['Key']}")
            except Exception as e:
                console.log(f"Error deleting RunLogs for {timestamp}: {e}")

        console.log(
            f"Cleanup complete: deleted {len(reports_to_delete)} old reports and associated RunLogs"
        )

    except Exception as e:
        console.log(f"Error during cleanup: {e}")
        console.log(traceback.format_exc())
        # Don't raise - cleanup is best-effort and shouldn't fail the execution


def completed_result(council, scraper_type, run_log, run_id=None, commit=None):
    """
    A scraper run that finished, as its report entry starts out.

    No run log text for a success, but enough for the report to say how
    long it took and what it produced: the commit it pushed, or None when
    nothing had changed.
    """
    return {
        "council": council,
        "scraper_type": scraper_type,
        "start_time": run_log.start.isoformat(),
        "statusCode": 200,
        "status": "completed",
        "end_time": run_log.end.isoformat(),
        "duration_seconds": run_log.duration.total_seconds(),
        "run_id": run_id,
        "commit": commit,
    }


def failed_result(
    council,
    scraper_type,
    run_log,
    error,
    bucket=None,
    rate_limited=False,
    console=None,
):
    """
    A scraper run that raised ``error``, as its report entry starts out.

    With a bucket, the run's full log is saved under runlogs/ and the entry
    points to it; the entry itself carries only a one-line error.
    """
    error_type = type(error).__name__
    # Just the exception type and its first line, no traceback
    error_message = str(error).split("\n")[0][:150]

    runlog_s3_key = None
    if bucket:
        timestamp = run_log.start.strftime("%Y%m%d-%H%M%S")
        runlog_s3_key = f"runlogs/{timestamp}/{council}-{scraper_type}.json"
        save_to_s3(
            bucket,
            runlog_s3_key,
            {
                "council": council,
                "scraper_type": scraper_type,
                "status": "rate_limited" if rate_limited else "failed",
                "start_time": run_log.start.isoformat(),
                "end_time": run_log.end.isoformat() if run_log.end else None,
                "duration_seconds": run_log.duration.total_seconds()
                if run_log.duration
                else 0,
                "error": run_log.error,
                "error_type": error_type,
                "error_message": error_message,
            },
            console or Console(file=sys.stderr),
        )

    return {
        "council": council,
        "scraper_type": scraper_type,
        "start_time": run_log.start.isoformat(),
        "statusCode": 429 if rate_limited else 500,
        "status": "failed",
        "error": "GHRateLimited" if rate_limited else f"{error_type}: {error_message}",
        "runlog_s3_key": runlog_s3_key,
    }


def build_report(results, execution_id, bucket=None):
    """
    The report on a run, from its scrapers' results.

    Returns the report as saved, under "report", beside its summary and the
    scrapers grouped by outcome, which the state machine's later steps use.
    """
    successful_scrapers = []
    failed_scrapers = []
    disabled_scrapers = []
    rate_limited_scrapers = []
    total_ran = 0

    for result in results:
        if not isinstance(result, dict):
            continue

        council = result.get("council", "unknown")
        scraper_type = result.get("scraper_type", "unknown")
        status = result.get("status", "unknown")
        status_code = result.get("statusCode", 200)

        total_ran += 1

        if status == "completed":
            successful_scrapers.append(
                {"council": council, "scraper_type": scraper_type}
            )
        elif status_code == 429:
            # GitHub rate limited
            rate_limited_scrapers.append(
                {
                    "council": council,
                    "scraper_type": scraper_type,
                    "error": result.get("error", "GHRateLimited"),
                }
            )
        elif status == "failed":
            failed_scrapers.append(
                {
                    "council": council,
                    "scraper_type": scraper_type,
                    "error": result.get("error", "Unknown error"),
                }
            )
        elif status == "disabled":
            disabled_scrapers.append({"council": council, "scraper_type": scraper_type})

    total_succeeded = len(successful_scrapers)
    summary = {
        "total_ran": total_ran,
        "total_succeeded": total_succeeded,
        "total_failed": len(failed_scrapers),
        "total_rate_limited": len(rate_limited_scrapers),
        "total_disabled": len(disabled_scrapers),
        "success_rate": f"{(total_succeeded / total_ran * 100) if total_ran > 0 else 0:.2f}%",
    }

    timestamp = datetime.datetime.now(datetime.UTC)
    report = {
        "execution_id": execution_id,
        "timestamp": timestamp.isoformat(),
        "summary": summary,
        "scrapers": [],
    }

    for result in results:
        if not isinstance(result, dict):
            continue

        scraper_entry = {
            "council": result.get("council", "unknown"),
            "scraper_type": result.get("scraper_type", "unknown"),
            "status": result.get("status", "unknown"),
            "status_code": result.get("statusCode", 200),
            "start_time": result.get("start_time"),
        }

        # What a successful run adds: how long it took and the commit
        # it pushed (null when nothing had changed), so the report
        # links to the repository the way the commit's trailers link
        # back to the report.
        for key in ("end_time", "duration_seconds", "run_id", "commit"):
            if key in result:
                scraper_entry[key] = result[key]

        if result.get("error"):
            scraper_entry["error"] = result.get("error")

        # Link to the detailed RunLog if there is one
        if result.get("runlog_s3_key"):
            scraper_entry["runlog_s3_key"] = result.get("runlog_s3_key")
            scraper_entry["runlog_s3_url"] = (
                f"s3://{bucket}/{result.get('runlog_s3_key')}"
            )

        report["scrapers"].append(scraper_entry)

    return {
        "timestamp": timestamp,
        "summary": summary,
        "successful_scrapers": successful_scrapers,
        "failed_scrapers": failed_scrapers,
        "rate_limited_scrapers": rate_limited_scrapers,
        "disabled_scrapers": disabled_scrapers,
        "report": report,
    }


def publish_report(bucket, built, console):
    """
    Save a report from build_report and prune the oldest past REPORTS_KEPT.
    Returns its key, or None if it couldn't be saved.
    """
    report = built["report"]
    timestamp_str = built["timestamp"].strftime("%Y%m%d-%H%M%S")
    key = f"run-reports/run-report-{timestamp_str}-{report['execution_id'][:8]}.json"

    if not save_to_s3(bucket, key, report, console):
        console.log("Failed to save run report to S3")
        return None

    console.log(f"Run report saved to S3: s3://{bucket}/{key}")
    console.log("Starting cleanup of old reports...")
    cleanup_old_reports(bucket, console, keep_count=REPORTS_KEPT)
    return key


def read_results(path):
    """The results a run's commands wrote to a --results-file, one per line."""
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def local_execution_id(now=None):
    """An execution id for a run outside Step Functions: local-<host>-<time>."""
    now = now or datetime.datetime.now(datetime.UTC)
    return f"local-{os.uname().nodename.split('.')[0]}-{now:%Y%m%d-%H%M%S}"


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m lgsf.run_reports")
    commands = parser.add_subparsers(dest="command", required=True)
    publish = commands.add_parser(
        "publish", help="Report a run's results as one run, as the dashboard reads"
    )
    publish.add_argument("results_file", help="The file given to --results-file")
    publish.add_argument(
        "--execution-id",
        required=True,
        help="The id the run's commands were given, so commits lead to the report",
    )
    args = parser.parse_args(argv)

    console = Console()
    bucket = os.environ.get("S3_REPORTS_BUCKET")
    if not bucket:
        parser.error("S3_REPORTS_BUCKET must be set")

    results = (
        read_results(args.results_file) if os.path.exists(args.results_file) else []
    )
    if not results:
        console.print("No results to report.")
        return 0

    built = build_report(results, args.execution_id, bucket)
    if not publish_report(bucket, built, console):
        return 2

    summary = built["summary"]
    console.print(f"{summary['total_succeeded']} of {summary['total_ran']} succeeded.")
    failures = built["failed_scrapers"] + built["rate_limited_scrapers"]
    for f in failures:
        console.print(
            f"[red]Failed:[/red] {f['council']} {f['scraper_type']}: {f['error']}"
        )
    # A failure fails whatever ran this, so whoever scheduled it hears.
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
