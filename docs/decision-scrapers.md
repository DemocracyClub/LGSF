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

Two list pages carry decisions and councils differ in which they populate, so
both are read and the results deduplicated by URL:

| Page | Carries |
| --- | --- |
| `mgDelegatedDecisions.aspx` with `DS=2` | Published delegated decisions |
| `mgListOfficerDecisions.aspx` | Officer decisions |

A council that lacks one of them is not an error; the other still runs.

### `CustomHTMLDecisionsScraper`

For everything else. It supplies `self.get_page(url)`, which fetches and
parses a page. Scaffold it with the `decisions_scraper_custom` template and
implement `get_decisions()` and `get_single_decision()`, exactly as for
minutes.

CMIS is not supported. Its decision pages are structured differently and no
scraper has been written for them.

## The scrape window

Decisions are scraped over a window ending today, set by one class attribute:

```python
years_back = 1
```

Only published decisions are collected. Forthcoming ones are proposals, not
decisions, and ModernGov lists them separately under `DS=1`.

**The window filters on publication date, not decision date.** A council can
publish a decision years after making it, so a record's `date` is routinely
outside the window — one published in December 2025 may have been decided in
July 2021. This is why decisions accumulate: filing by decision date means
records land outside any window a later run would look at.

Widening `years_back` makes some councils time out; the list page is generated
per request over whatever range you ask for.

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

## Known limitations

- **ModernGov sends no HTTP validators** on decision pages — no `ETag` or
  `Last-Modified` — so conditional requests never produce a 304 and every run
  re-fetches every decision page in the window. The plumbing is there for
  sources that do send them.
- **No CMIS support.**
- **No pagination.** A council with more decisions in the window than its list
  page will render in one response will lose the remainder.
