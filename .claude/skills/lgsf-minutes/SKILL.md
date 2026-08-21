---
name: lgsf-minutes
description: Write and fix LGSF minutes scrapers - committee meetings and the documents attached to them, the rolling scrape window, and how documents are stored separately from metadata.
when_to_use: Working on a council's minutes.py, on lgsf/minutes/, on meeting or document scraping, or picking up a council that needs a minutes scraper.
paths: scrapers/*/minutes.py, lgsf/minutes/**
---

# Minutes scrapers

Minutes scrapers produce a set of `MeetingBase` objects, each with the
documents linked from that meeting. Full reference in
`docs/minutes-scrapers.md`.

For running them and diagnosing failures, see the `lgsf-run` skill.

## Always use --skip-documents while iterating

```bash
uv run python manage.py minutes --council KIR -v --skip-documents
```

Documents are found and recorded but not downloaded. A single council can be
hundreds of megabytes otherwise.

## Choosing a class

| Council's site | Class | Notes |
| --- | --- | --- |
| ModernGov | `ModGovMinutesScraper` | `base_url` must be the directory containing `mgWebService.asmx` |
| CMIS | `CMISMinutesScraper` | `base_url` is the meetings page; the name varies per install |
| Anything else | `CustomHTMLMinutesScraper` | implement `get_meetings` and `get_single_meeting` |

Scaffold with `--template minutes_scraper_modgov`, `minutes_scraper_cmis` or
`minutes_scraper_custom`.

## The scrape window

Meetings are scraped over a rolling window set by `weeks_back` (4) and
`weeks_forward` (1). It looks mostly backwards on purpose: minutes are
published days or weeks after a meeting, so revisiting is how they get picked
up once they appear.

This means **a scraper finding zero meetings is not necessarily broken.** Over
a quiet period, or a recess, there may genuinely be none. Check by asking the
council's API or calendar directly for the same window before concluding
anything.

## Documents

Metadata and files are stored separately:

- The meeting JSON goes to `data/<COUNCIL>/Minutes/json/` and is meant for
  version control. Each document it links records a `content_hash`, a
  `storage_key` and the backend holding it — but **never a resolved URL**,
  which would be specific to whoever ran the scrape.
- The files go to `data/<COUNCIL>/documents/` through a pluggable document
  backend.
- A document already in the store is not downloaded again, tracked through
  `_index.json`. That index is a cache, not a source of truth: entries are
  checked against the store before being used to skip.

**Store documents before saving the record's JSON.** The JSON is serialised
from the document dicts, and storing a document is what adds the hash and
storage key to them. The other order records documents pointing at nothing.

## Storage

Minutes use `StorageMode.ACCUMULATE`: a meeting that has dropped out of the
window is still a fact, so runs add to what is stored rather than replacing
it. Do not change this to replace.

## Related

- `docs/minutes-scrapers.md` — classes, window, skipping, documents
- `docs/running-scrapers.md` — running and diagnosing
