---
name: lgsf-add-scraper-type
description: Add a whole new data type to LGSF - the thing councillors, minutes and decisions each are - rather than a scraper for one council. Covers the framework seams a new type has to touch.
when_to_use: Adding a new kind of scraped data such as decisions or committees. Not for adding a scraper for a single council.
---

# Adding a scraper type

A data type is a package under `lgsf/` with a model, a base scraper, CMS
subclasses and a command — `lgsf/councillors/`, `lgsf/minutes/` and
`lgsf/decisions/` are the ones that exist.

**Read `docs/adding-a-scraper-type.md` before starting.** It lists every file
and framework seam involved.

## Decide these first

They are hard to change later:

- **Is the latest scrape the whole truth, or a historical record?** Sets
  `storage_mode` to `StorageMode.REPLACE` or `.ACCUMULATE`. Getting it wrong
  on a historical type silently deletes anything outside the scrape window.
- **Does it download files?** Those go to the document store, not the metadata
  store.
- **What is the natural scrape window?**
- **What identifies one record?** It must be stable across runs — a source
  system id, not a row position.

## What a new type touches

```
lgsf/<type>/          model, scrapers, command, exceptions, tests
lgsf/conf/__init__.py add "<type>" to settings.APPS
lgsf/templates/       scaffolding templates
lgsf/tests/test_runner.py  asserts the full app list
scrapers/<CODE>/      per-council <type>.py and metadata block
```

`lgsf/metadata/models.py` needs **no** change — services are keyed by name, so
a type works as soon as a council's `metadata.json` declares it. Do not add a
per-service field there.

## Related

- `docs/adding-a-scraper-type.md` — the full procedure
- `.claude/skills/lgsf-decisions` — a worked example of a type
