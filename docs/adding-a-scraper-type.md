# Adding a new scraper type to LGSF

A reference for adding a new *data type* to the framework.

To add a scraper for a **single council** on an existing type, see
[running-scrapers.md](running-scrapers.md) instead.

Throughout, `<type>` is the lowercase plural name used on the command line
(`minutes`, `committees`), and `<Type>` is the capitalised form used for storage
directories (`Minutes`, `Committees`).

---

## 0. Decide these before writing code

**Storage mode.** Is the latest scrape the whole truth, or a historical record?
Councillors are the former — someone who has left should vanish, so each run
replaces everything (`StorageMode.REPLACE`). Minutes are the latter — a meeting
from last year is still a fact, so each run adds to what is stored
(`StorageMode.ACCUMULATE`). Sets `storage_mode` in step 3. Hard to change later:
`REPLACE` deletes the history you would want to migrate.

**Documents.** If the type stores PDFs or other binaries, they go to the
document store, not the metadata store. See step 8.

**Scrape window.** Councillors have none. Minutes use a rolling window
(`weeks_back` / `weeks_forward`). Whatever you choose must be a class attribute
so a single council can override it.

**Identifier.** Becomes the model's `identifier`, and drives deduplication and
filenames. Must be stable across runs: an id from the source system, not a row
position or a hash of the title.

---

## 1. Files you will create and edit

**Create — the type's package:**

```
lgsf/<type>/__init__.py          re-exports models and exceptions
lgsf/<type>/models.py            the record dataclass
lgsf/<type>/exceptions.py        Skip<Thing>Exception
lgsf/<type>/scrapers.py          Base<Type>Scraper + one class per CMS
lgsf/<type>/commands.py          the manage.py subcommand
lgsf/<type>/tests/__init__.py
lgsf/<type>/tests/fixtures/      saved real responses
lgsf/<type>/tests/test_*.py
```

**Edit — three framework seams:**

```
lgsf/conf/__init__.py            add "<type>" to settings.APPS
lgsf/templates/helpers.py        scaffolding templates, see step 9
lgsf/tests/test_runner.py        the help-text assertion lists every app
```

`lgsf/metadata/models.py` needs no change — services are keyed by name.

**Per council, once the type works:**

```
scrapers/<CODE>/<type>.py        a Scraper subclass
scrapers/<CODE>/metadata.json    a services.<type> block
```

---

## 2. The model — `lgsf/<type>/models.py`

A plain dataclass. The framework needs four methods from it.

```python
import json
from dataclasses import dataclass, field
from pathlib import Path

from slugify import slugify


@dataclass
class CommitteeBase:
    url: str
    identifier: str
    title: str
    date: str
    # Fields the scraper fills in after construction use init=False so they
    # don't have to be passed to add_committee(). Keep them out of hash and
    # equality: two scrapes of the same record are the same record.
    status: str = field(init=False, hash=False, compare=False)
    documents: list = field(init=False, hash=False, compare=False)

    def __repr__(self):
        return "<Committee: {} on {}>".format(self.title, self.date)

    def __hash__(self):
        return hash(self.identifier)

    def __eq__(self, other):
        return (
            issubclass(type(other), CommitteeBase)
            and self.identifier == other.identifier
        )

    def as_file_name(self):
        """Stable, filesystem-safe stem. No extension."""
        return f"{slugify(self.date)}-{slugify(self.identifier)}"

    def as_dict(self):
        """What lands in the JSON file."""
        out = {
            "url": self.url,
            "status": getattr(self, "status", None),
            "documents": getattr(self, "documents", []),
        }
        # Fields taken verbatim from the source get a raw_ prefix, marking
        # them as unnormalised. This is a framework-wide convention.
        for attr in ["identifier", "title", "date"]:
            out["raw_{}".format(attr)] = getattr(self, attr)
        return out

    def as_json(self):
        return json.dumps(self.as_dict(), indent=4, sort_keys=True)

    @classmethod
    def from_storage(cls, filename: Path, session):
        """Rebuild a record from stored JSON. Mirror of as_dict()."""
        data = json.loads(session.open(filename))
        status = data.pop("status", None)
        documents = data.pop("documents", None)
        for k in list(data.keys()):
            if k.startswith("raw_"):
                data[k[4:]] = data.pop(k)
        record = cls(**data)
        if status:
            record.status = status
        if documents is not None:
            record.documents = documents
        return record
```

- `as_file_name()` must be unique per record within a council. Minutes include
  the committee, because the same committee id and date can recur. Collisions
  overwrite silently.
- Use `getattr(self, "field", default)` in `as_dict()` for every `init=False`
  field. A bare attribute access raises `AttributeError` during error
  reporting, masking the real failure.

---

## 3. The base scraper — `lgsf/<type>/scrapers.py`

```python
import abc

from lgsf.aws_lambda.run_log import RunLog
from lgsf.committees import CommitteeBase
from lgsf.committees.exceptions import SkipCommitteeException
from lgsf.scrapers import ScraperBase
from lgsf.storage.backends.base import StorageMode


class BaseCommitteesScraper(ScraperBase):
    tags = []
    class_tags = []
    ext = "html"

    # --- required by ScraperBase.check(), asserted at construction ---
    scraper_object_type = "Committees"   # storage directory name
    service_name = "committees"          # key in metadata.json services block

    # --- see step 0 ---
    storage_mode = StorageMode.ACCUMULATE

    def __init__(self, options, console):
        super().__init__(options, console)
        self.committees = set()
        self.new_data = True

    @abc.abstractmethod
    def get_committees(self):
        """Yield raw per-record blobs (Tag, dict, str - your choice)."""

    @abc.abstractmethod
    def get_single_committee(self, committee_data):
        """Turn one raw blob into a record via self.add_committee()."""

    def add_committee(self, url, identifier, title, date):
        assert title, f"No title for {url}"
        assert identifier, f"No identifier for {url}"
        committee = CommitteeBase(
            url, identifier=identifier, title=title, date=date
        )
        self.committees.add(committee)
        return committee

    @property
    def get_tags(self):
        return self.tags + self.class_tags

    @property
    def report_items(self):
        """Feeds the --report flag. Must return a list."""
        return list(self.committees)

    def run(self, run_log: RunLog):
        for committee_data in self.get_committees():
            try:
                committee = self.get_single_committee(committee_data)
                self.process_committee(committee, committee_data)
            except SkipCommitteeException:
                continue

        self.finalize_storage(run_log)
        self.report()

    def process_committee(self, committee, raw):
        # Download files HERE, before save_json: storing a document adds its
        # hash and storage key to the record, and the JSON is serialised from
        # those. Saving first records documents with no location.
        self.save_raw(f"{committee.as_file_name()}.{self.ext}", raw)
        self.save_json(committee)

    def report(self):
        self.console.log(f"Found {len(self.committees)} committees")
```

### What you inherit from `ScraperBase`

| Method | Notes |
|---|---|
| `self.get(url, extra_headers=None)` | Raises for 4xx/5xx. **Not** for 304 |
| `self.get_text(url, ...)` | Normalises `.text` across clients |
| `self.get_conditional(url, etag=, last_modified=, ...)` | Sends `If-None-Match` / `If-Modified-Since` |
| `self.response_status(response)` | Returns an int on every client |
| `self.response_header(response, "etag")` | Returns a str; wreq gives bytes |
| `self.save_raw(filename, content)` | Stages into `raw/` |
| `self.save_json(obj)` | Stages `obj.as_json()` into `json/` |
| `self.storage_session` | Metadata store, staged until commit |
| `self.document_storage` | Document store, lazily created |
| `self.base_url` | From `services.<type>.base_url` in metadata.json |
| `self.council_metadata` | The whole `CouncilMetadata` |
| `self.finalize_storage(run_log)` | Commits. Call once, at the end of `run()` |

Also set on the class: `http_lib` (`wreq` default, or `requests` / `httpx` /
`playwright`), `timeout` (default 30 — CMIS month calendars need 120),
`verify_requests`, `disabled`, `use_proxy`, `extra_headers`.

**Do not add HTTP retry logic.** Lambda retries at the invocation level.

---

## 4. Exceptions — `lgsf/<type>/exceptions.py`

```python
class SkipCommitteeException(Exception):
    """Raised by a scraper to drop a record from the run entirely."""
```

Add a `NotModifiedException` too if you implement conditional fetching, so
`run()` can count "skip this, it's junk" separately from "skip this, it's
unchanged".

---

## 5. The command — `lgsf/<type>/commands.py`

```python
from rich.table import Table

from lgsf.commands.base import PerCouncilCommandBase


class Command(PerCouncilCommandBase):
    command_name = "committees"   # must equal the package name

    def output_report(self):
        """Rich table shown by --report."""
        table = Table(title="Scraped Committees")
        table.add_column("Date", style="cyan", no_wrap=True)
        table.add_column("Title", style="green")

        for item in sorted(self.scraped_items, key=lambda d: d.date):
            table.add_row(item.date or "N/A", item.title or "N/A")

        if self.scraped_items:
            self.console.print(table)
        else:
            self.console.print("[yellow]No committees found[/yellow]")
```

`PerCouncilCommandBase` supplies `--council`, `--all-councils`, `--tags`,
`--refresh`, `--check-only`, `--list-missing`, `--list-disabled`,
`--list-failing`, `--exclude-missing`, `--report`, `--verbose` and
`--unpretty`. You implement only `output_report`, which renders
`self.scraped_items` (populated from each scraper's `report_items`).

`command_name` must match the package directory name — `load_command()` imports
`<type>.commands` by that string. Add `AWSInvokableMixin` only when the type
will run on Lambda.

---

## 6. Register the type

**`lgsf/conf/__init__.py`** — add to `settings.APPS`. Without this the
subcommand does not exist:

```python
self.APPS = ("councillors", "minutes", "committees", "templates", ...)
```

**`lgsf/metadata/models.py`** — nothing. `CouncilMetadata` keys services by
name, so a new service works as soon as a council's `metadata.json` declares
it. `get_service_metadata("committees")` returns a `ServiceData` for any name,
empty when the council has no config for it. Do not add a per-service field or
branch to this model.

**`lgsf/tests/test_runner.py`** — the help-text test asserts the full app list.
Add your subcommand in `settings.APPS` order.

---

## 7. Per-council configuration

`scrapers/<CODE>/metadata.json`:

```json
{
    "services": {
        "committees": {
            "base_url": "https://democracy.example.gov.uk",
            "cms_type": "ModernGov"
        }
    }
}
```

`scrapers/<CODE>/<type>.py` — the class must be named `Scraper`:

```python
from lgsf.committees.scrapers import ModGovCommitteesScraper


class Scraper(ModGovCommitteesScraper):
    pass
```

To populate hundreds of councils at once, match on **hostname**: a council's
services nearly always share a domain, so its existing `councillors` entry is
the best source. Do not fall back to fuzzy name matching — it produces
confident, wrong mappings between similarly named organisations. Leave a
council unconfigured rather than guessing, protect already-configured entries,
and run every council afterwards to check what you generated.

---

## 8. If your type downloads files

Documents do **not** go through `self.storage_session`. That is the metadata
store, destined for git.

```python
from lgsf.storage.documents import get_document_storage_backend  # via ScraperBase

stored = self.document_storage.write(key, content)
record_entry = stored.as_dict()
# -> storage_key, storage_backend, content_hash, content_length
```

The resolved URL is deliberately not recorded: it is specific to wherever the
scrape ran, so the same document would produce a different value for every
person who scraped it. Store the key — the document's path within its backend —
and call `document_storage.url_for(key)` when you need a URL.

1. **Skip what you already have.** Check `self.document_storage.exists(key)`
   before downloading, or every run re-downloads everything.
2. **Verify the index against the store.** Keep a `_index.json` in the metadata
   store mapping source URL to storage key, but treat it as a cache: confirm
   the file is really there before skipping, so a deleted file costs a
   re-download rather than a lost document.
3. **Store documents before saving the record's JSON.** See step 3.
4. **Offer a way to skip downloads.** Add a `--skip-documents` flag in the
   command's `add_arguments` and honour it in the scraper, as minutes does, so
   parsing can be checked without transferring every file.
5. **Keep the key portable.** Unique per council, stable across runs, free of
   anything machine-specific. Source-system attachment ids are ideal; fall back
   to a per-record index where the source has none (CMIS URLs are opaque
   encrypted query strings).

---

## 9. Scaffolding templates — `lgsf/templates/helpers.py`

```python
class ModGovCommitteesTemplate(BaseTemplate):
    required_fields = ["base_url"]
    file_name = "committees.py"

    template = """from lgsf.committees.scrapers import ModGovCommitteesScraper


class Scraper(ModGovCommitteesScraper):
    base_url = "$base_url"

    """


TEMPLATES = {
    ...,
    "committees_scraper_modgov": ModGovCommitteesTemplate,
}
```

Then `python manage.py templates --council ABC --template committees_scraper_modgov
--context base_url https://...` scaffolds a council file.

---

## 10. Tests

- **Fixtures are real saved responses**, trimmed to the relevant markup, in
  `lgsf/<type>/tests/fixtures/`. Do not invent markup.
- **Construct scrapers with `__new__`** to bypass `ScraperBase.__init__`, which
  wants council metadata, a storage backend and an HTTP client:

  ```python
  scraper = ModGovCommitteesScraper.__new__(ModGovCommitteesScraper)
  scraper.base_url = "https://democracy.example.gov.uk"
  scraper.committees = set()
  scraper.console = FakeConsole()
  scraper.storage_session = FakeStorageSession()
  ```

  A `FakeStorageSession` needs `write`, `write_bytes` and an `open` that raises
  `FileNotFoundError` for absent files.
- **Call the real methods.** Fake the HTTP layer and drive the actual code
  path; do not reproduce a method's body inline in the test.
- **Add an abstract-class test** mirroring `lgsf/<type>/tests/test_base_class.py`.
- No network in tests, ever.

---

## 11. Verify, in this order

```bash
uv run ruff check lgsf/ && uv run ruff format --check lgsf/
uv run pytest lgsf/ -q

python manage.py                             # subcommand listed?
python manage.py committees --council ABC -v  # one known-good council
python manage.py committees --council ABC     # again: is the second run cheap?

# Metadata and scraper files agree, across every council at once
python manage.py metadata validate --service committees
python manage.py metadata list-cms --service committees --csv
```

`metadata validate --service <type>` works for a new type with no changes: it
checks that a council declaring `services.<type>` also has a `<type>.py` and
vice versa, that `base_url` is set, and that the scraper's base class agrees
with the recorded `cms_type`. Councils that don't do the service are reported
as not applicable rather than as failures.

Then check by hand:

- `data/ABC/Committees/json/` — is the JSON shaped as intended?
- `data/ABC/documents/` — are files there, and does the JSON point at them?
- Run twice and confirm `ACCUMULATE` types keep the first run's output.
- Drop a hand-written file into the data directory and re-run: with
  `ACCUMULATE` it must survive, with `REPLACE` it must vanish.

**Run against a live council.** Source-shape bugs pass unit tests; only real
output shows them.

---

## 12. Source gotchas

- **A CMS view that looks pageable may not be.** The CMIS list view accepts a
  date parameter and ignores it, always rendering the current month;
  `MeetingCalendarPublicNoJava` with `/Date/<yyyy-mm-dd>/` is the one that
  works. Check what a source returns for an out-of-range request before
  building on it.
- **`timeout` at the default is often too low.** Some council pages take 40+
  seconds against a default of 30.
- **Servers may send no validators.** ModernGov's web service sends no `ETag`
  or `Last-Modified`, so conditional requests never produce a 304 there.
- **CSS class names are not reliable keys.** CMIS uses the same `Value Venue`
  class for the status and the venue. Key off label text instead.

---

## 13. Reference: how the pieces find each other

- `manage.py <type>` → `CommandRunner` checks `settings.APPS` → `load_command()`
  imports `<type>.commands` and takes its `Command` class.
- `Command.handle()` → `load_scraper(code, command_name)` loads
  `scrapers/<CODE>/<type>.py` and takes its `Scraper` class, asserting it
  subclasses `ScraperBase`.
- `ScraperBase.__init__` → `CouncilMetadata.for_council(code)` →
  `get_service_metadata(service_name)` → `self.base_url`.
- `ScraperBase.__init__` → `get_storage_backend(..., scraper_object_type,
  storage_mode)` → session rooted at `data/<CODE>/<Type>/`.
- `self.document_storage` → `get_document_storage_backend(...)` → rooted at
  `data/<CODE>/documents/`.
