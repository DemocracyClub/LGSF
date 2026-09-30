# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Added

- **Register of Interests scraper type** (`interests`) — new data type scraped
  from every council's published register of interests.
  - `lgsf/interests/` package: `models.py`, `scrapers.py`, `exceptions.py`,
    `commands.py`, and a full test suite with fixture files.
  - `BaseInterestsScraper` — abstract base; `StorageMode.REPLACE` (interests
    are a current snapshot, not a historical record).
  - `ModGovInterestsScraper` — concrete implementation for ModernGov councils.
    Discovers each councillor's register via their profile page
    (`mgUserInfo.aspx`), handling both HTML tables and direct PDF/document
    links (e.g. `mgConvert2PDF.aspx`).
  - `interests_scraper_modgov` scaffolding template registered in
    `lgsf/templates/helpers.py`.
  - `"interests"` added to `settings.APPS` and the `test_runner` help-text
    assertion.
  - `--skip-documents` flag: finds and records document links without
    downloading, matching the `minutes` behaviour.
  - Document deduplication: `document_storage.exists(key)` prevents
    re-downloading on every run; `document_storage.describe(key)` backfills
    storage metadata (`storage_key`, `content_hash`, `content_length`,
    `storage_backend`) so the JSON is identical whether the file was fetched
    this run or a previous one.
  - Interest categories stored as `{"category": ..., "rows": [{"You": ..., "Spouse/partner": ...}]}` —
    each row is a dict keyed by the column header, making the
    councillor/partner mapping explicit without needing to zip separately.
  - Download counters (`documents_downloaded`, `documents_skipped`,
    `documents_linked`) reported at the end of each run.
  - Per-council `interests.py` and `metadata.json` `services.interests` blocks
    added for all ModernGov councils that share a `base_url` with their
    existing `councillors` service.

### Changed

- `docs/running-scrapers.md` — updated to include `interests` in the list of
  supported scraper types, `--skip-documents` flag table, output directory
  layout, and scaffolding template list.
