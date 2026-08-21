---
name: lgsf-run
description: Run LGSF scrapers, find councils that need work, and diagnose one that is failing. Covers manage.py commands shared by every data type - councillors, minutes and any added later.
when_to_use: Running a scraper, working out why one is broken, finding councils with no scraper or a failing one, or checking metadata against scrapers.
---

# Running LGSF scrapers

LGSF scrapes UK council websites into open data. Each council has a package in
`scrapers/CODE-slug/` holding a `metadata.json` and one Python file per data
type. Every data type is a `manage.py` subcommand taking the same options.

Read `docs/running-scrapers.md` for the full reference.

## Two things to know first

**Run a single council with `-v`.** Without it the command catches the
exception, prints a summary table and exits 0, so a broken scraper looks like
a quiet one. Across many councils `-v` will *not* stop the run.

**`base_url` comes from `metadata.json`, not the scraper class.** A scraper
that sets `base_url` on the class and nowhere else runs against nothing. Look
at `services.<type>.base_url` in the council's `metadata.json`.

## Commands

```bash
# One council, watching what it does
uv run python manage.py <type> --council KIR -v

# Everything. Councils run concurrently; one failing never stops the rest
uv run python manage.py <type> --all-councils

# What has no scraper, what failed last time it ran here
uv run python manage.py <type> --list-missing
uv run python manage.py <type> --list-failing

# Metadata and scraper files agree?
uv run python manage.py metadata validate --service <type>
uv run python manage.py metadata list-cms --service <type> --csv
```

Useful options: `--workers N` (how many scrapers at once, default 4; use 1 to
watch one), `--report` (table of what was scraped), `--tags`,
`--skip-documents` (minutes only).

## Where things land

```
data/<COUNCIL>/<Type>/json/      normalised records
data/<COUNCIL>/<Type>/raw/       raw responses
data/<COUNCIL>/<Type>/runlog.json  how the last run went, locally only
data/<COUNCIL>/documents/        downloaded files
```

`--list-failing` reads those run logs first, so it mostly knows about councils
run on this machine. Where there are no local logs it falls back to the
production report, which only councillors has — so for other types an empty
result means "nothing has been run here", not "nothing is failing".

## Diagnosing a broken scraper

Most breakage is configuration, not parsing. In order:

1. Run it with `-v` and read the actual exception.
2. Does the host resolve, and serve what you expect in a browser?
3. Does the council's **councillors** `base_url` work where this one doesn't?
   They are usually the same host; if they differ the councillors one is more
   likely current.
4. For ModernGov, `{base_url}/mgWebService.asmx/GetCommittees` should return
   XML. A 404 usually means `base_url` includes a page name that needs
   stripping — it must be the directory *above* `mgWebService.asmx`.
5. For CMIS, the meetings page name varies between installs: `Meetings.aspx`,
   `MeetingsCalendar.aspx`, `CalendarofMeetings.aspx`.

If the site is down or blocking, say so rather than working around it.

### What the failure usually turns out to be

| Symptom | Cause | Fix |
| --- | --- | --- |
| Timeout on an `http://` URL | Site is HTTPS-only now | `base_url` to `https://` in `metadata.json` |
| SSL / certificate error | Cert chain the HTTP client won't trust | `verify_requests = False` on the scraper class |
| Name resolution failure | Council moved CMS or domain | Find the new site — see the councillors skill |
| 404 on a URL that used to work | URL structure changed | New `base_url` in `metadata.json` |
| 403 Forbidden | Cloudflare or another WAF | `http_lib = "playwright"`, or `use_proxy = True` |
| `AttributeError: 'NoneType' has no attribute …` | Page structure changed | Reselect against the live page; check for a JSON endpoint first |
| 409 Conflict from ModernGov | Server-side fault | Usually not fixable from here |

## Working through a batch of failing scrapers

- **One council at a time**, committed on its own as
  `Fix ABC (Council Name): what changed`.
- **Only touch that council's files** under `scrapers/CODE-slug/`. If a fix
  seems to need a change in `lgsf/`, that is a framework change — raise it
  separately rather than folding it into a scraper fix.
- **Fixing is not writing.** A council with no scraper is a different job;
  `--list-missing` finds those.
- **Time-box each one.** Some breakage needs a human — anti-bot protection
  and server-side faults especially. Record what you found and move on.

## Checks before proposing a change

```bash
uv run ruff check lgsf/ && uv run ruff format --check lgsf/
uv run pytest lgsf/ -q
```

Tests never hit the network. Fixtures are real saved responses in
`lgsf/<type>/tests/fixtures/`.

## Related

- `docs/running-scrapers.md` — the full reference
- `.claude/skills/lgsf-councillors` — councillor scraper specifics
- `.claude/skills/lgsf-minutes` — minutes scraper specifics
