"""
Work out where each council's minutes backfill should start, from what
Poteris already holds.

Poteris have been scraping council meetings for a few years, and their
data will be backfilled into ours separately. So our own backfill only
needs to cover from where theirs ends to today. And it always goes back at
least a year, so every council gets something whether or not Poteris has
it.

For each of our councils with a minutes scraper this finds the matching
Poteris council (by the host of the site both scrape, and by name where
several councils share a host) and asks their public API for the newest
meeting they hold. The council's start date is then the earlier of

- that meeting's date, less a margin: calendar pages list meetings months
  ahead, so the newest meeting Poteris holds can be later than their last
  scrape (Aberdeen City's is August 2026, while the one before it is March),
  and
- a year before today.

A council Poteris doesn't have starts a year back.

Writes ``council,since,poteris_id,poteris_newest,matched_by`` to a CSV,
which ``manage.py minutes --since-file`` reads. Uses the API without a
token; it's read-only and public:

    uv run python -m scripts.poteris_since [--out minutes-since.csv] \\
        [--margin-days 90] [--years-back 1]
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import requests

API = "https://councilgateway.poteris.co.uk/council-api"
SCRAPERS = Path("scrapers")


@dataclass
class Council:
    code: str
    name: str
    hosts: set


def normalise(name: str) -> str:
    """'Adur District Council' and 'Adur' to 'adur'."""
    name = name.lower().replace("&", "and")
    for word in (
        "metropolitan borough council",
        "royal borough of",
        "london borough of",
        "borough council",
        "district council",
        "county council",
        "city council",
        "council",
        "city of",
        "the ",
    ):
        name = name.replace(word, " ")
    return " ".join(name.replace(",", " ").split())


def our_councils(
    scrapers: Path = SCRAPERS, scraper_type: str = "minutes"
) -> list[Council]:
    """Our councils with a scraper of this type, with the hosts they scrape."""
    found = []
    for directory in sorted(scrapers.iterdir()):
        if not (directory / f"{scraper_type}.py").exists():
            continue
        metadata_path = directory / "metadata.json"
        if not metadata_path.exists():
            continue
        metadata = json.loads(metadata_path.read_text())
        ee = metadata.get("everyelectiion_data") or {}
        hosts = {
            urlparse(service["base_url"]).netloc.lower()
            for service in (metadata.get("services") or {}).values()
            if (service or {}).get("base_url")
        }
        found.append(
            Council(
                code=directory.name.split("-")[0],
                name=ee.get("common_name") or ee.get("official_name") or directory.name,
                hosts=hosts,
            )
        )
    return found


def poteris_councils(session: requests.Session) -> list[dict]:
    councils, page = [], 1
    while True:
        response = session.get(
            f"{API}/councils", params={"page": page, "per_page": 100}, timeout=30
        )
        response.raise_for_status()
        data = response.json()
        councils.extend(data["items"])
        if page * data["per_page"] >= data["total"]:
            return councils
        page += 1


def match(ours: list[Council], theirs: list[dict]) -> dict[str, tuple[dict, str]]:
    """{our code: (their council, how matched)}"""
    by_host: dict[str, list[dict]] = {}
    by_name: dict[str, list[dict]] = {}
    for council in theirs:
        host = urlparse(council.get("base_scrape_url") or "").netloc.lower()
        if host:
            by_host.setdefault(host, []).append(council)
        if council.get("name"):
            by_name.setdefault(normalise(council["name"]), []).append(council)

    # The names must agree, whatever else does: a host alone matched
    # Bournemouth, Christchurch and Poole to Fife, and Broadland to South
    # Norfolk, whose democracy site it shares. A council left unmatched
    # only loses the chance of an earlier start; it still goes back a year.
    matched = {}
    for council in ours:
        name = normalise(council.name)
        on_host = [c for host in council.hosts for c in by_host.get(host, [])]
        named_on_host = [c for c in on_host if normalise(c.get("name") or "") == name]
        if len(named_on_host) == 1:
            matched[council.code] = (named_on_host[0], "host+name")
        elif len(by_name.get(name, [])) == 1:
            matched[council.code] = (by_name[name][0], "name")
    return matched


def newest_meeting(session: requests.Session, poteris_id: int) -> datetime.date | None:
    """Their newest meeting for a council: the list is newest first."""
    response = session.get(
        f"{API}/meetings", params={"council_id": poteris_id, "per_page": 1}, timeout=30
    )
    response.raise_for_status()
    items = response.json()["items"]
    if not items or not items[0].get("date"):
        return None
    return datetime.date.fromisoformat(items[0]["date"][:10])


def start_date(
    newest: datetime.date | None,
    today: datetime.date,
    margin_days: int,
    years_back: int,
) -> datetime.date:
    """The earlier of a year (or years_back) ago and their newest, less the margin."""
    year_ago = today - datetime.timedelta(days=365 * years_back)
    if newest is None:
        return year_ago
    return min(year_ago, newest - datetime.timedelta(days=margin_days))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", type=Path, default=Path("minutes-since.csv"))
    parser.add_argument("--margin-days", type=int, default=90)
    parser.add_argument("--years-back", type=int, default=1)
    args = parser.parse_args(argv)

    today = datetime.date.today()
    session = requests.Session()
    session.headers["User-Agent"] = "LGSF backfill planning (democracyclub.org.uk)"

    ours = our_councils()
    theirs = poteris_councils(session)
    matched = match(ours, theirs)
    print(
        f"{len(ours)} councils with a minutes scraper; {len(theirs)} at Poteris; {len(matched)} matched"
    )

    def newest_for(code):
        council, _ = matched[code]
        return newest_meeting(session, council["id"])

    codes = sorted(matched)
    with ThreadPoolExecutor(4) as pool:
        newest = dict(zip(codes, pool.map(newest_for, codes)))

    rows = []
    for council in ours:
        poteris, how = matched.get(council.code, (None, ""))
        date = newest.get(council.code)
        rows.append(
            {
                "council": council.code,
                "since": start_date(
                    date, today, args.margin_days, args.years_back
                ).isoformat(),
                "poteris_id": poteris["id"] if poteris else "",
                "poteris_newest": date.isoformat() if date else "",
                "matched_by": how,
            }
        )
    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    year_ago = start_date(None, today, args.margin_days, args.years_back)
    earlier = [r for r in rows if r["since"] < year_ago.isoformat()]
    print(
        f"{len(earlier)} councils start before {year_ago} because Poteris's data ends earlier"
    )
    print(f"Written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
