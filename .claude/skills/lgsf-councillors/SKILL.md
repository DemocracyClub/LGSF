---
name: lgsf-councillors
description: Write and fix LGSF councillor scrapers - the councillor scraper classes, what a Councillor object needs, and how to skip rows that are not councillors.
when_to_use: Working on a council's councillors.py, on lgsf/councillors/, or on councillor data generally.
paths: scrapers/*/councillors.py, lgsf/councillors/**
---

# Councillor scrapers

Councillor scrapers produce a set of `CouncillorBase` objects, one per
councillor. Full reference in `docs/councillor-scrapers.md`.

For running them, the dev loop and diagnosing failures, see the `lgsf-run`
skill.

## Choosing a class

| Council's site | Class | Usually needs |
| --- | --- | --- |
| ModernGov | `ModGovCouncillorScraper` | nothing but a `base_url` in metadata |
| CMIS | `CMISCouncillorScraper` | nothing but a `base_url` in metadata |
| Anything else, one page | `HTMLCouncillorScraper` | CSS selectors and `get_single_councillor` |
| Anything else, paginated | `PagedHTMLCouncillorScraper` | as above plus a next-page selector |
| A JSON API | `JSONCouncillorScraper` | a `get_single_councillor` over the parsed feed |

Scaffold one rather than writing it from scratch:

```bash
uv run python manage.py templates --council ABC \
    --template councillor_scraper_html \
    --context base_url https://example.gov.uk/councillors
```

## The contract

`get_single_councillor` must return a councillor built with
`self.add_councillor()`, which requires `url`, `identifier`, `name`, `party`
and `division`. All five are asserted, so a missing one fails loudly.

`identifier` must be stable across runs — an id from the source system, not a
row number. It becomes the filename, so an unstable one silently creates
duplicates.

Where a row isn't a councillor — an inline header row, or a neighbouring
council's members on a shared site — raise `SkipCouncillorException` and the
loop moves on.

## When a council moves CMS

A name resolution failure, or a 404 on a `base_url` that used to work, usually
means the council has moved — often onto ModernGov. Search for
"<council name> councillors", find the live page, then read the CMS off the
URL:

| URL contains | CMS | Switch the class to |
| --- | --- | --- |
| `mgMemberIndex.aspx`, or any `mg*.aspx` | ModernGov | `ModGovCouncillorScraper` |
| `cmis.uk.com`, `/cmis5/`, or a `cmis.` host | CMIS | `CMISCouncillorScraper` |
| neither | bespoke | `HTMLCouncillorScraper`, plus selectors |

Moving to ModernGov or CMIS usually shrinks the scraper body to `pass` — both
work from `base_url` alone.

Put the new URL in the council's `metadata.json` under
`services.councillors.base_url`, **not** on the class. A `base_url` set on the
class is overwritten from metadata at runtime, so a scraper that only sets it
there runs against nothing.

## Storage

Councillors use `StorageMode.REPLACE`: each run clears what came before,
because the current set is the whole truth and someone who has left should
disappear. Do not change this to accumulate.

## Related

- `docs/councillor-scrapers.md` — classes, the councillor object, skipping
- `docs/running-scrapers.md` — running and diagnosing
