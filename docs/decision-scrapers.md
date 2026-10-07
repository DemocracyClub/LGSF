# Decision scrapers

The scraper classes for council decisions and the documents attached to them.
For running them see [running-scrapers.md](running-scrapers.md).

A decision scraper produces a set of `DecisionBase` objects. Each carries the
decision text itself, which is stored in the record rather than as a document:
it is small, and it is the point of the record.

All classes read `base_url` from the council's `metadata.json`
(`services.decisions.base_url`), not from the class.

**Starting a council from scratch?** Look at its `councillors` entry in
`metadata.json` first. A council's services nearly always share a host, so if
councillors is ModernGov then decisions probably is too.

### `ModGovDecisionsScraper`

For ModernGov sites. Usually all you need:

```python
from lgsf.decisions.scrapers import ModGovDecisionsScraper


class Scraper(ModGovDecisionsScraper):
    pass
```

`base_url` is the site root, e.g. `https://democracy.kirklees.gov.uk`.

Unlike minutes, this reads rendered HTML rather than a web service:
**ModernGov's `mgWebService.asmx` has no decisions method.** It exposes
meetings, committees, councillors and webcasts only.

Two list pages are read and the results deduplicated by URL:

| Page | Carries |
| --- | --- |
| `mgDelegatedDecisions.aspx` with `DS=2` | Published delegated decisions |
| `mgListOfficerDecisions.aspx` | Officer decisions — a subset of the above |

A council that lacks one of them is not an error; the other still runs.

## Officer decisions and delegated decisions

These are not two kinds of record. A local authority may delegate a function
to a committee, a sub-committee **or an officer** (Local Government Act 1972,
s.101(1)(a)), so an officer decision is a delegated decision whose delegate is
an officer. The recording duty for one is in the Openness of Local Government
Bodies Regulations 2014 reg 7; for executive decisions taken by members it is
the Executive Arrangements Regulations 2012 regs 12 and 13.

The endpoints reflect that containment. Over the same date range:

| Council | Delegated | Officer | On the officer list only |
| --- | ---: | ---: | ---: |
| Kirklees | 657 | 48 | 0 |
| Dorset | 740 | 606 | 0 |
| Durham | 603 | 601 | 0 |

So the officer page adds **no coverage**. It is still read, because appearing
on it is the only signal the source gives for which decisions those are, and
that is recorded as `is_officer_decision`.

Read `is_officer_decision` as "the council listed this among its officer
decisions", not as a legal classification. A council with no officer page
yields `False` throughout, and the 2012 and 2014 regulations are England-only
while this scrapes all four nations.

`DS=2` is not a coverage risk either: across all nine `DS` values Kirklees
returns 658 decisions in a year, and `DS=2` alone returns 657.

### `CustomHTMLDecisionsScraper`

For everything else. It supplies `self.get_page(url)`, which fetches and
parses a page. Scaffold it with the `decisions_scraper_custom` template and
implement `get_decisions()` and `get_single_decision()`, exactly as for
minutes.

CMIS is not supported. Its decision pages are structured differently and no
scraper has been written for them.

## The scrape window

Decisions are scraped over a window ending today: a year by default, or from
`--since` for a backfill.

```python
years_back = 1
window_months = 6
```

Only published decisions are collected. Forthcoming ones are proposals, not
decisions, and ModernGov lists them separately under `DS=1`.

**The window filters on publication date, not decision date.** A council can
publish a decision years after making it, so a record's `date` is routinely
outside the window — one published in December 2025 may have been decided in
July 2021. This is why decisions accumulate: filing by decision date means
records land outside any window a later run would look at.

### Slices

The window is read in calendar-aligned slices of `window_months`, newest
first, one list request per page per slice. ModernGov generates the list per
request over whatever range is asked for, and a wide range makes it give up:
Kirklees answers eleven years with an error page after two minutes, but three
years in eleven seconds.

A list request that fails slowly, or with a timeout or a 5xx, is split in half
and each half read, down to `min_split_days`. A *quick* 404 is a council that
doesn't have that page, and is not split.

Slices are aligned to the calendar so that a slice is the same slice from one
run to the next. That lets a slice be recorded in `_index.json` as **complete**
once its list pages were read in full and every decision on it was stored with
all its documents, or is settled. A complete slice is not listed again for
`relist_after_days` (30); after that it is listed again, which is cheap
because everything on it is settled, and catches decisions published late with
an old publication date. Slices that can still change — any ending within
`settled_after_months` — are never complete.

## A backfill

```bash
uv run python manage.py decisions --all-councils --discover-since
```

`--discover-since` finds where each council's history starts and backfills
from there. How far back that is varies: Kirklees has nothing before 2013,
and around 11,000 decisions since. `--since YYYY-MM-DD` sets one date for
every council instead; the two can't be combined.

Discovery walks forward from `discover_floor` (2000-01-01) in
`discover_chunk_years` (2) chunks, reading both list pages, and stops at the
first chunk with any decisions on it: its oldest publication date is the
start. Forward, not back from today, because old ranges are empty or sparse
and ModernGov answers them quickly, while recent years are dense and slow.
Gaps in a council's history don't matter, since the first chunk with
anything on it holds the oldest decision whatever follows. For Kirklees that
is seven chunks, fourteen fast requests.

Before searching, discovery checks that one of the list pages returns a
real page (not a 404) for the last month. Some sites answer every page with an error — Enfield and
Somerset redirect everything to `mgError.aspx` — and searching their history
would only send the backfill back to 2000 to fail there. A council that fails
the check gets the default window and nothing is remembered.

A chunk that can't be read might hold the oldest decisions, so discovery
starts from that chunk: too early costs a few empty slices, too late would
lose decisions silently. The answer is kept in `_index.json` as
`discovered_since`, unless the run gave up part way, so a rerun doesn't
search again. A council with no decisions at all falls back to the default
window.

A backfill can be stopped and rerun: completed slices are skipped without a
request, settled decisions in an incomplete slice are skipped without a
request, and a document already in the document store is not downloaded
again even if the index never recorded it. Run it again after it finishes to
pick up anything that failed.

**Progress is committed as it goes**, after every slice and every
`checkpoint_every` (50) decision pages, where the storage backend can do that
cheaply (`supports_checkpoints`: local storage yes, GitHub no, since each
commit there is a pull request). An interrupted run loses at most the
decisions since the last checkpoint.

`--skip-documents` works for a backfill, but every decision it stores with
documents is marked incomplete, and so is its slice, so a later run without
the flag goes back for them.

## Writing to S3

For a long run that others should be able to follow, write to S3 instead of
`data/`. The layout under the prefix is exactly that of `data/`, so anyone
with read access can sync it at any point and get a data directory LGSF can
read, or carry on scraping into:

```bash
export LGSF_STORAGE_BACKEND=s3
export LGSF_S3_BUCKET=<bucket>
export LGSF_S3_PREFIX=data          # optional
export AWS_PROFILE=<profile>        # or any other way boto3 finds credentials
uv run python manage.py decisions --all-councils --discover-since --workers 12
```

```bash
aws s3 sync s3://<bucket>/data ./data     # repeat to pick up new data
```

Documents go to S3 too, beside the metadata, unless
`LGSF_DOCUMENT_STORAGE_BACKEND` says otherwise.

It behaves as local storage does. Records are staged in memory and uploaded
at each checkpoint (every 50 decisions and each slice), so S3 is a few
minutes behind the scrape; documents are uploaded as they are downloaded.
The index goes up only after the records it names, so a sync mid-run never
has an index pointing at files that aren't there. There are no commits or
pull requests as with GitHub: a checkpoint is just uploads.

Each council's keys are listed once, on first use, and existence checks are
answered from that listing, so a run makes about one request to S3 per file
it writes, plus a listing per council. Each document records its SHA-256 in
the object's metadata, so recovering one after a crash is a HEAD rather than
a download.

The runner needs `s3:ListBucket` on the bucket and `s3:GetObject` and
`s3:PutObject` under the prefix, plus `s3:DeleteObject` only for data types
stored in REPLACE mode (not decisions). Readers need `s3:ListBucket` and
`s3:GetObject`.

Don't run two machines on the same council at once: each would overwrite
the other's index.

`--list-failing` reads run logs from `data/` and doesn't see ones on S3;
they are at `<prefix>/<COUNCIL>/Decisions/runlog.json`.

## Watching a long run

A local run over several councils at once, in a terminal, shows a live table
with a row per running council: the slice it is on and how many are left,
decisions done in the slice, documents downloaded, kept and failed, data
downloaded, requests and retries, and what it is doing right now with how
long it has been doing it. A request running past its timeout shows red:
that is a stalled worker, not a busy one. Finished councils drop into the
totals line, which also names any that failed or gave up.

```bash
uv run python manage.py decisions --all-councils --discover-since --workers 12
```

The table only exists there. Scrapers report to `self.progress`, which is a
`NullProgress` that does nothing unless the command replaces it, and it never
does in Lambda, for a single council, or when output isn't a terminal. A
Lambda invocation runs one council with nobody watching and shares nothing
with any other invocation, and a nightly run is short enough not to need it.
The same goes for the per-host throttle: it is shared between councils
within one process, which matters locally, where councils that share a site
run in parallel threads, and is simply per-council in Lambda.

## Request rate and retries

```python
request_interval = 1  # seconds between requests to one host
retries = 3
```

`request_interval` is the minimum gap between the starts of two requests to
the same host, shared across every council in the run: several councils share
one ModernGov site, and `--workers` runs councils in parallel. Override it with
`--request-interval`.

A request that times out, loses its connection, or gets 429, 500, 502, 503 or
504 is retried with backoff of 5, 10 then 20 seconds, or whatever
`Retry-After` asks for, up to five minutes. Retries are off for other scraper
types (`ScraperBase.retries = 0`), which rely on Lambda retrying a failed
invocation; decisions run locally, and skip a failed page rather than failing,
so nothing else would retry it.

At one request a second a council like Kirklees is a few hours of decision
pages for a full backfill, plus its documents.

## The record

```python
decision = self.add_decision(
    url,
    identifier=identifier,
    title=title,
    date=date,
    decision_maker=decision_maker,
)
decision.status = "Recommendations Approved"
decision.is_key_decision = False
decision.is_subject_to_call_in = False
decision.is_officer_decision = True
decision.publication_date = "2025-12-10"
decision.purpose = "To seek approval for ..."
decision.text = "That approval be given to ..."
decision.documents = [
    {
        "title": "Background Cabinet Report",
        "url": "https://.../documents/s67732/report.pdf",
        "attachment_id": "s67732",
    }
]
```

`identifier` must be stable across runs. For ModernGov it is the `ID`
parameter of the decision URL. It ends up in the filename, so an unstable one
silently creates duplicates.

## Reading a ModernGov decision page

Three things on these pages need undoing, and all three are silent if missed:

- **List links end with a hidden `ref: NNNN` span.** Left in, every title
  reads `...Railway Street, Huddersfieldref: 13567`.
- **Document links carry their file size the same way.** Left in, every
  document is titled `Report PDF 119 KB`.
- **The text is pasted from Word and hard-wrapped in the source.** Read
  straight it gains a newline every few words, mid-sentence, and lands in
  version control that way. Paragraph breaks are real and are kept; the
  `&nbsp;` paragraphs Word puts between them are not.

Labels vary between installs. A decision maker may appear as `Decision
Maker:` or only as `Decided at meeting:`; both are read.

## Documents, and where they go

Metadata and document files are stored separately, exactly as for minutes.

The decision JSON goes to `data/<COUNCIL>/Decisions/json/` and is meant for
version control — including the decision text. Each document it links records
a `content_hash`, a `storage_key` and the backend holding it, but not a
resolved URL, which would be specific to whoever ran the scrape.

The files themselves go to `data/<COUNCIL>/documents/` through the pluggable
document backend. ModernGov document URLs carry an `sNNNN` attachment id,
which is used as the storage key so the same document is not stored twice.

A document already in the store is not downloaded again. `--skip-documents`
skips downloading entirely, which is what you want while iterating on parsing.

## Storage

Decisions use `StorageMode.ACCUMULATE`: a decision made last year is still a
fact once it drops out of the window, so runs add to what is stored rather
than replacing it. Do not change this to replace.

## What a second run re-fetches

A decision is amended, if at all, in the months just after it is published: a
status moves on, or a report is attached late. Past that it is settled, so one
already in storage is not fetched again.

```python
settled_after_months = 3
```

The clock runs from the **publication** date, not the date the decision was
taken — publishing is what starts the amendment window, and a decision
published last week may have been made years ago. The list page carries that
date, so a settled decision is identified without fetching its page at all.

Kirklees, a year's window: 454 decisions, 396 settled, 31 seconds instead of
215.

Four things are never treated as settled, because each would mean losing a
record rather than saving a request:

- A decision the index does not name a stored file for. A run that failed
  records validators but never a filename, so a previous failure is always
  retried however old it is.
- A decision stored without all its documents, because a download failed or
  the run was `--skip-documents`. Its index entry is marked `incomplete`. A
  document that answers 404 or 410 is recorded on the record as
  `unavailable` and does not count as missing.
- A decision whose stored file has gone. The index is a cache; the file it
  names is opened before its word is taken.
- A decision the list page gives no publication date for.

Set `settled_after_months = None` to re-fetch everything in the window.

This is a fallback for a source that offers nothing better. Conditional
requests are still made for everything that *is* fetched, so if ModernGov
starts sending `ETag` or `Last-Modified` the 304s take over for recent
decisions with no change here — the index records validators and filename
side by side.

## One unreadable page does not lose the run

Each decision is its own page fetch and a council can have several hundred in
the window, so a transient failure on one is routine. A decision page that
can't be read is logged, counted and skipped, and the count is reported at the
end. Letting it propagate would abandon the run before storage was finalised
and lose every decision scraped up to that point.

## Known limitations

- **ModernGov sends no HTTP validators** on decision pages — no `ETag` or
  `Last-Modified` — so conditional requests never produce a 304. Recent
  decisions are therefore re-fetched and re-parsed in full on every run; see
  above for what that costs and what avoids it.
- **No CMIS support.**
- **No pagination**, but none has been seen: Kirklees renders 3,267
  decisions over two years in one response, and three one-year ranges give
  exactly the decisions of the three-year range. Slicing keeps each response
  far smaller than that.
- **Some sites error on every page.** Enfield and Somerset redirected every
  page, decisions or not, to `mgError.aspx` when checked on 2026-10-07, and
  Rutland takes a minute over any request. A council like that costs a
  worker up to about 40 minutes before the run gives up on it.
- **Councils that share a site scrape the same decisions.** Adur and
  Worthing, Babergh and Mid Suffolk, Broadland and South Norfolk, Eastbourne
  and Lewes, and South Hams and West Devon each share one ModernGov install,
  so each pair stores the same decisions twice, once under each council.
