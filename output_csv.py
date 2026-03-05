# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "python-dateutil",
# ]
# ///
"""
Output councillor data as CSV, filtering to current councils only.

Usage:
    uv run output_csv.py > councillors.csv
    uv run output_csv.py --all > all_councillors.csv  # include non-current
"""

import csv
import glob
import json
import sys
from datetime import date
from pathlib import Path

from dateutil.parser import parse


def is_current_council(metadata: dict) -> bool:
    """Check if a council is current based on its metadata."""
    today = date.today()

    # Check for dates in everyelectiion_data first (new format)
    everyelectiion_data = metadata.get("everyelectiion_data", {})
    end_date = everyelectiion_data.get("end_date") or metadata.get("end_date")
    start_date = everyelectiion_data.get("start_date") or metadata.get("start_date")

    if end_date and parse(end_date).date() < today:
        return False
    if start_date and parse(start_date).date() > today:
        return False
    return True


def get_current_council_ids() -> set[str]:
    """Get set of council IDs that are currently active."""
    current_ids = set()
    scrapers_dir = Path("scrapers")

    for metadata_file in scrapers_dir.glob("*/metadata.json"):
        council_id = metadata_file.parent.name.split("-")[0].upper()
        try:
            with open(metadata_file) as f:
                metadata = json.load(f)
            if is_current_council(metadata):
                current_ids.add(council_id)
        except (json.JSONDecodeError, OSError):
            continue

    return current_ids


field_names = [
    "council_id",
    "raw_division",
    "raw_identifier",
    "email",
    "url",
    "raw_name",
    "raw_party",
]

csvout = csv.DictWriter(sys.stdout, fieldnames=field_names)
csvout.writeheader()

# Check for --all flag
include_all = "--all" in sys.argv

# Get current councils (unless --all is specified)
current_council_ids = None if include_all else get_current_council_ids()

for file_name in glob.glob("./data/**/json/*.json"):
    council_id = file_name.split("/")[-3]

    # Skip non-current councils unless --all
    if current_council_ids is not None and council_id not in current_council_ids:
        continue

    councillor = json.load(open(file_name))
    for k in list(councillor.keys()):
        if k not in field_names:
            del councillor[k]
    councillor["council_id"] = council_id
    csvout.writerow(councillor)

    # council_id = file_name.split("/")[-3]
    # if not council_id in councillor_counter:
    #     councillor_counter[council_id] = 0
    # councillor_counter[council_id] += 1

    # with open(file_name) as f:
    #     json_data = json.loads(f.read())
    #     if not json_data["FaceDetails"]:
    #         continue
    #     face = json_data["FaceDetails"][0]
    #     out_dict = json_data['councillor_json']
    #     out_dict.update({
    #         "gender": face["Gender"]["Value"],
    #         "age_low": face["AgeRange"]["Low"],
    #         "age_high": face["AgeRange"]["High"],
    #         "smile": face["Smile"]["Value"],
    #         "glasses": face["Eyeglasses"]["Value"],
    #         "beard": face["Beard"]["Value"],
    #         "happy": any(
    #             [
    #                 x
    #                 for x in face["Emotions"]
    #                 if x["Type"] == "HAPPY" and x["Confidence"] > 70
    #             ]
    #         )
    #     })
    #     csvout.writerow(out_dict)

# for council_id, count in councillor_counter.items():
#     print(",".join((council_id, str(count))))
