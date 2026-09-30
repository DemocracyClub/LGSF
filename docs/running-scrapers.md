# Running scrapers

How to run scrapers, find work, and diagnose one that is
failing. For what a scraper of each type looks like, see
[councillor-scrapers.md](councillor-scrapers.md) and
[minutes-scrapers.md](minutes-scrapers.md).

Three types of scraper are supported: `councillors`, `minutes`, and `interests`.
All take the same options.

To run a single council:

```
python manage.py councillors --council KIR
python manage.py minutes --council KIR
python manage.py interests --council KIR
```

Where `KIR` is the council ID from the
[mySociety lookup table](https://pages.mysociety.org/uk_local_authority_names_and_codes/).

To run everything:

```
python manage.py minutes --all-councils --skip-documents
```

Several councils are scraped at once — `--workers` sets how many. One
council failing never stops the run: each outcome is
printed as a summary table at the end, and recorded in
`data/<COUNCIL>/<Type>/runlog.json` beside the data that run produced. Each
council keeps its most recent run only, and the scraper's console output is
kept for the ones that failed.

`--list-failing` reads those run logs, so after a full run it will tell you
what is currently broken. It only knows about councils you have actually run
on this machine.

A run over every council takes a while, and for minutes without
`--skip-documents` will download a great deal — see below.

Useful options while working on a scraper:

| Option | What it does |
| --- | --- |
| `-v` | Verbose. Logs every URL fetched. Running a **single** council it also re-raises the exception, so the failure reaches the exit code |
| `--workers N` | How many councils to scrape at once (default 4). `1` streams one scraper's output as it runs |
| `--report` | Print a table of what was scraped |
| `--skip-documents` | Minutes and interests only. Find and record documents without downloading them |
| `--list-missing` | Councils with no scraper of this type |
| `--list-disabled` | Councils whose scraper is marked `disabled` |
| `--list-failing` | Councils that failed the last time you ran them here |

Output lands in a `data` directory:

```
data/KIR/Councillors/     json/ and raw/, one file per councillor
data/KIR/Minutes/         json/ and raw/, one file per meeting
data/KIR/Interests/       json/ and raw/, one file per councillor's register
data/KIR/documents/       PDFs and other files linked from meetings or interests
```

Minutes accumulate: each run adds to what's already stored rather than
replacing it, because a meeting from last year is still a fact. Councillors
and interests replace, because someone who has left the council should
disappear, and the current register is the whole truth.

## Working on a scraper

Install pre-commit hooks:
`$ pre-commit install`

## Finding something to work on

```
python manage.py minutes --list-missing      # councils with no minutes scraper
python manage.py minutes --list-disabled     # scrapers someone turned off
python manage.py metadata validate --service minutes

python manage.py interests --list-missing    # councils with no interests scraper
python manage.py interests --list-disabled
python manage.py metadata validate --service interests
```

When you've picked a council, its existing `councillors` entry in
`scrapers/<CODE>/metadata.json` is the best clue to what its other services
need: a council on ModernGov for councillors is almost always on ModernGov for
meetings too, usually at the same base URL.

`metadata validate` checks that every council declaring a service has a
matching scraper file, that `base_url` is set, and that the scraper's base
class agrees with the recorded `cms_type`.

There is no feed of which scrapers are failing for a given service — the
`--list-failing` reports what failed the last time you ran each council
locally, so run the scrapers first: `manage.py <type> --all-councils
--skip-documents`. For councillors, where no local runs exist it falls back to
the production dashboard.

## Adding a scraper for a council

1. Find the council's register code using the link above.

2. Check whether the council already has a package in `scrapers/`. Most do —
   they're named `CODE-slug`.

3. Scaffold the scraper:

   ```
   python manage.py templates --council KIR \
       --template minutes_scraper_modgov \
       --context base_url https://democracy.kirklees.gov.uk
   ```

   Templates: `minutes_scraper_modgov`, `minutes_scraper_cmis`,
   `minutes_scraper_custom`, `councillor_scraper_modgov`,
   `councillor_scraper_cmis`, `councillor_scraper_html`,
   `interests_scraper_modgov`.

4. Add the service to the council's `metadata.json`:

   ```json
   {
       "services": {
           "minutes": {
               "base_url": "https://democracy.kirklees.gov.uk",
               "cms_type": "ModernGov"
           }
       }
   }
   ```

   The scraper reads `base_url` from here, so a scraper that only sets it on
   the class will run against nothing.

5. Test it:

   ```
   python manage.py minutes --council KIR -v --skip-documents
   ```

## Fixing a broken scraper

Run the single council with `-v` first. Without it the command catches the
exception, prints a summary table and exits 0, so a broken scraper looks like
a quiet one. Across many councils `-v` won't stop the run — read the run log
file instead.

Most breakage is configuration rather than code. In a sweep of all 337
configured minutes scrapers, the large majority of failures were a wrong or
stale `base_url` rather than a parsing problem. Worth checking in order:

- Does the host resolve, and does it serve what you expect in a browser?
- Does the council's **councillors** `base_url` work where the minutes one
  doesn't? They're usually the same host, and if they differ the councillors
  one is more likely to be current.
- For ModernGov, `{base_url}/mgWebService.asmx/GetCommittees` should return
  XML. If it 404s, `base_url` probably includes a page name that needs
  stripping — it should be the directory *above* `mgWebService.asmx`.
- For CMIS, the meetings page name varies between installs: `Meetings.aspx`,
  `MeetingsCalendar.aspx`, `CalendarofMeetings.aspx`.

If the site is simply down or blocking you, say so in the issue rather than
working around it — some councils' sites are genuinely unreliable.
