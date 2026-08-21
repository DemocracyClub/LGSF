# Local Government Scraper Framework

This is an experimental project that should be considered alpha and unstable.

The Local Government Scraper Framework is a set of tools for scraping UK local
government websites. It was made for fun to see what a toolkit for people who
wanted to turn government websites in to open data would look like.

If you want to actually use this project, it's recommended you also
[join the Democracy Club Slack](https://slack.democracyclub.org.uk/) to talk
about it with us. Everything is likely to change, and this code is in no way
supported.

## How it fits together

**A council is a directory.** Each of the ~440 councils has a package in
`scrapers/`, named `CODE-slug`, holding a `metadata.json` and one Python file
per kind of data being scraped.

**A data type is a package.** `lgsf/councillors/` and `lgsf/minutes/` each
define a model, a base scraper, CMS-specific subclasses, and a command. Adding
more types means adding a package like them — see
[docs/adding-a-scraper-type.md](docs/adding-a-scraper-type.md).

Two types are supported:

| Type | Command | Scrapes | Scraper classes | Storage |
| --- | --- | --- | --- | --- |
| [Councillors](docs/councillor-scrapers.md) | `manage.py councillors` | The current set of councillors | ModernGov, CMIS, HTML, paged HTML, JSON | Replaced each run |
| [Minutes](docs/minutes-scrapers.md) | `manage.py minutes` | Committee meetings and their documents | ModernGov, CMIS, custom HTML | Added to each run |

A council's scraper subclasses the base for whatever content management system
that council uses:

```python
from lgsf.minutes.scrapers import ModGovMinutesScraper


class Scraper(ModGovMinutesScraper):
    pass
```

Most UK councils run one of two systems, ModernGov or CMIS. This framework
also supports writing custom HTML scrapers.

**Configuration lives in metadata, not code.** A scraper reads its `base_url`
from the council's `metadata.json`, under `services.<type>`. The same council
can be on different systems for different data types.

**The HTTP client is pluggable.** Council sites vary in what they will answer,
so a scraper picks its client by setting `http_lib` on the class:

| `http_lib`   | Client | |
|--------------| --- | --- |
| `wreq`       | Rust client, default | Sends the TLS and header fingerprint of Firefox 133 |
| `requests`   | `requests.Session` | |
| `httpx`      | `httpx.Client` | Follows redirects |
| `playwright` | Headless Chromium | Runs JavaScript, for sites behind Incapsula or Cloudflare |

Other class attributes that change how requests are made: `verify_requests`
to skip TLS verification, `timeout` (default 30), `extra_headers`, and
`use_proxy`, which routes through `PROXY_URL` from the environment. `wreq` has
no proxy support, so a scraper setting both gets `requests` instead.

**Everything is driven from `manage.py`**, one subcommand per data type:

```
python manage.py minutes --council KIR
python manage.py councillors --all-councils
```

**Output is files.** Scraped data lands in `data/<COUNCIL>/<Type>/` as raw
responses and normalised JSON, with documents beside it in
`data/<COUNCIL>/documents/`. Storage is pluggable: the same run can write to
the local filesystem or commit to a per-council GitHub repository.

## Install

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```
git clone git@github.com:DemocracyClub/LGSF.git
cd LGSF
uv sync
```

Check it works by scraping a single council:

```
uv run python manage.py minutes --council ADU -v --skip-documents
```

That should report a handful of meetings and write them to
`data/ADU/Minutes/`. No API keys or config are needed to run scrapers locally.

Install the pre-commit hooks if you intend to contribute:

```
uv run pre-commit install
```

## Documentation

| Document | What it covers |
| --- | --- |
| [docs/running-scrapers.md](docs/running-scrapers.md) | Running scrapers, the options that matter, finding work, fixing a broken scraper |
| [docs/councillor-scrapers.md](docs/councillor-scrapers.md) | Councillor scraper classes and the objects they produce |
| [docs/minutes-scrapers.md](docs/minutes-scrapers.md) | Minutes scraper classes, the scrape window, and document storage |
| [docs/adding-a-scraper-type.md](docs/adding-a-scraper-type.md) | Adding a whole new data type, not a council |

## For LLMs and coding agents

`.claude/skills/` holds task-scoped guides for agents working on this repo.
Each is a single `SKILL.md` covering the conventions, commands and constraints
for one kind of task.

| Skill | Read it when |
| --- | --- |
| [lgsf-run](.claude/skills/lgsf-run/SKILL.md) | Running a scraper, diagnosing a failing one, or finding councils that need work |
| [lgsf-councillors](.claude/skills/lgsf-councillors/SKILL.md) | Working on a council's `councillors.py`, or on `lgsf/councillors/` |
| [lgsf-minutes](.claude/skills/lgsf-minutes/SKILL.md) | Working on a council's `minutes.py`, or on meeting and document scraping |
| [lgsf-add-scraper-type](.claude/skills/lgsf-add-scraper-type/SKILL.md) | Adding a new data type, not a scraper for one council |
