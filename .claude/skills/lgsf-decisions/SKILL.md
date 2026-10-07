---
name: lgsf-decisions
description: Write and fix LGSF decision scrapers - council officer and delegated decisions, the decision text, and the documents attached to them.
when_to_use: Working on a council's decisions.py, on lgsf/decisions/, or on decision scraping generally.
paths: scrapers/*/decisions.py, lgsf/decisions/**
---

# Decision scrapers

Decision scrapers produce a set of `DecisionBase` objects, each carrying the
decision text and the documents linked from it. Full reference in
`docs/decision-scrapers.md`.

For running them and diagnosing failures, see the `lgsf-run` skill.

## Always use --skip-documents while iterating

```bash
uv run python manage.py decisions --council KIR -v --skip-documents
```

One council is hundreds of megabytes of attachments otherwise.

## Choosing a class

| Council's site | Class | Notes |
| --- | --- | --- |
| ModernGov | `ModGovDecisionsScraper` | `base_url` is the site root |
| Anything else | `CustomHTMLDecisionsScraper` | implement `get_decisions` and `get_single_decision` |

Scaffold with `--template decisions_scraper_modgov` or
`decisions_scraper_custom`. CMIS is not supported.

## ModernGov has no decisions API

`mgWebService.asmx` covers meetings, committees, councillors and webcasts
only. Decisions are read from rendered HTML, from two list pages —
`mgDelegatedDecisions.aspx` with `DS=2`, and `mgListOfficerDecisions.aspx`.
Both are read and deduplicated by URL.

## Officer decisions are a subset, not a second type

A function may be delegated to a committee, a sub-committee or an officer
(LGA 1972 s.101(1)(a)), so an officer decision is a delegated decision whose
delegate is an officer. Measured over the same range, the officer page is a
strict subset of the delegated one — 48 of 657 for Kirklees, 606 of 740 for
Dorset, none outside it in either case.

It is read anyway, because appearing on it is the only signal for which
decisions those are, recorded as `is_officer_decision`. Read that field as
"the council listed this as an officer decision", not as a legal
classification: a council with no officer page yields `False` throughout.

## Three things the pages do that need undoing

All three are silent if missed, and all three end up in version control:

- **List links end with a hidden `ref: NNNN` span**, which reads as part of
  the title.
- **Document links carry their file size the same way**, so a document ends
  up titled `Report PDF 119 KB`.
- **Decision text is pasted from Word and hard-wrapped in the source**, so
  reading it straight puts a newline mid-sentence every few words.

Labels vary between installs: a decision maker may be `Decision Maker:` or
only `Decided at meeting:`.

## The window filters on publication date

`years_back` (1) is applied to when a decision was *published*, not when it
was made. A council can publish years late, so a record's `date` is routinely
outside the window — this is normal, not a bug.

Only published decisions are scraped. Forthcoming ones are proposals.

The window is read in calendar-aligned 6-month slices (`window_months`),
because one request over many years makes ModernGov give up. A list request
that fails slowly is split in half and retried; a quick 404 is a missing page.
Don't go back to a single request over the whole window.

## Backfills, checkpoints and request rate

`--since YYYY-MM-DD` widens the window for a backfill. `--discover-since`
instead finds each council's oldest decision (walking forward from 2000 in
two-year chunks to the first with anything on it) and starts there; the
answer is kept in `_index.json` as `discovered_since`. A slice read in full,
with every decision stored or settled, is recorded as complete in
`_index.json` and skipped for 30 days, so a backfill can be stopped and rerun.
Progress is committed after every slice and every 50 decision pages, on
backends with `supports_checkpoints` (local, not GitHub).

Decisions set `request_interval = 1` (per host, shared across workers) and
`retries = 3` for timeouts, connection errors and 429/5xx. Retries are off in
`ScraperBase` for everything else, which relies on Lambda retries.

## A second run only re-fetches recent decisions

A decision is amended, if at all, in the months just after publication, so one
older than `settled_after_months` (3) that is already stored is not fetched
again. The clock runs from the **publication** date, which the list page
carries, so no request is made to decide this.

Never treated as settled: a decision with no stored file recorded (so a
previous failure is always retried), one whose stored file has gone, one
marked `incomplete` because a document is missing (a failed download, or
`--skip-documents`), or one with no publication date. A 404/410 document is
recorded as `unavailable` and doesn't count as missing. Set `settled_after_months = None` to re-fetch
everything.

Conditional requests are still made for whatever is fetched, so `ETag` and
`Last-Modified` take over automatically if ModernGov ever sends them.

## Progress is local only

A local run of several councils in a terminal shows a live Rich table
(`lgsf/scrapers/progress.py`). Scrapers report to `self.progress`, a no-op
`NullProgress` everywhere else, Lambda included: don't rely on it for
anything but display.

## The text goes in the record, not the document store

The decision text is small and is the point of the record, so it lands in
`data/<COUNCIL>/Decisions/json/` for version control. Attachments go to
`data/<COUNCIL>/documents/` through the document backend, keyed by their
`sNNNN` attachment id.

**Store documents before saving the record's JSON.** The JSON is serialised
from the document dicts, and storing a document is what adds the hash and
storage key to them. The other order records documents pointing at nothing.

## Storage

Decisions use `StorageMode.ACCUMULATE`: a decision that has dropped out of
the window is still a fact, so runs add to what is stored rather than
replacing it. Do not change this to replace.

## Related

- `docs/decision-scrapers.md` — classes, the window, the record, documents
- `docs/running-scrapers.md` — running and diagnosing
