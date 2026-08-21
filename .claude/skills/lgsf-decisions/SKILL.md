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

Widening the window makes some councils time out; the list page is generated
per request over whatever range is asked for.

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
