# Running scrapers

How to run scrapers, find work, and diagnose one that is
failing. For what a scraper of each type looks like, see
[councillor-scrapers.md](councillor-scrapers.md) and
[minutes-scrapers.md](minutes-scrapers.md).

Two types of scraper are supported: `councillors` and `minutes`. Both take the
same options.

To run a single council:

```
python manage.py councillors --council KIR
python manage.py minutes --council KIR
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

A council that fails commits nothing, whether it raised partway through or
finished and found implausibly little: whatever its last good run stored is
left as it was. The same holds in the daily AWS run, where a failed council
leaves its GitHub repository untouched.

`--list-failing` shows two lists. The first is what failed in the last
scheduled production run, from the dashboard's per-type feed at
`https://democracyclub.github.io/lgsf-dashboard/api/failing/<type>.json`
(councillors and minutes have one; a type production doesn't run yet has
none). The second is from those local run logs, and only knows about councils
you have actually run on this machine. Re-running one council here updates its
local log, not the production list, which changes after the next scheduled
run.

A run over every council takes a while, and for minutes without
`--skip-documents` will download a great deal — see below.

Useful options while working on a scraper:

| Option | What it does |
| --- | --- |
| `-v` | Verbose. Logs every URL fetched. Running a **single** council it also re-raises the exception, so the failure reaches the exit code |
| `--workers N` | How many councils to scrape at once (default 4). `1` streams one scraper's output as it runs |
| `--report` | Print a table of what was scraped |
| `--skip-documents` | Minutes only. Find and record documents without downloading them |
| `--list-missing` | Councils with no scraper of this type |
| `--list-disabled` | Councils whose scraper is marked `disabled` |
| `--list-failing` | Councils failing in the last production run, then those that failed the last time you ran them here |

Output lands in a `data` directory:

```
data/KIR/Councillors/     json/ and raw/, one file per councillor
data/KIR/Minutes/         json/ and raw/, one file per meeting
data/KIR/documents/       PDFs and other files linked from meetings and
                          decisions, one copy of each, named by content
```

Minutes accumulate: each run adds to what's already stored rather than
replacing it, because a meeting from last year is still a fact. Councillors
replace, because someone who has left the council should disappear.

The GitHub backend, which the daily AWS run uses, writes the same layout
into one repository per council in the LGSF-Data organisation: `main` of
`LGSF-Data/KIR` holds `Councillors/json/`, `Councillors/raw/` and so on.
Each data type has its own directory so a run of one type can't add to or
delete another's files. A councillors run replaces exactly the contents of
`Councillors/` and leaves the rest of the repository alone.

Each run that changes something makes exactly one commit on `main`, pushed
fast-forward; a run that finds nothing new makes none, and a run that fails
makes none. Two runs of the same council overlapping can't overwrite each
other: the second push is rejected and that run fails. The commit message
carries git trailers naming the run, so a commit can be traced to the run
that made it and to that day's run report:

```
Updated KIR councillors

LGSF-Run-Id: 3f9a1c2b7d4e
LGSF-Execution-Id: scraper-all-20261023-060000
LGSF-Scraper-Type: Councillors
```

`git log --format='%h %(trailers:key=LGSF-Run-Id,valueonly)'` lists them.
Repositories
written before this layout had `json/` and `raw/` at the root, and older
history had other arrangements again; `scripts/migrate_github_layout.py`
rewrote every commit so that the files are at the same paths throughout,
which is what a reader walking the commits as a log needs. Its docstring
says how to run it, and running it again is harmless.

## Where the output goes

`LGSF_STORAGE_BACKEND` picks the metadata store; `local` is the default.

| Value | Where | Notes |
| --- | --- | --- |
| `local` | `data/<COUNCIL>/<Type>/` | Default. Plain files, no history |
| `git` | one bare repository per council under `LGSF_GIT_REMOTE` | Production's GitHub path, on this machine |
| `github` | `LGSF-Data/<COUNCIL>` on GitHub | What the daily AWS run uses; needs GitHub App credentials |
| `s3` | `LGSF_S3_BUCKET` | Checkpointing types' production store |

Documents (PDFs and the like) have their own setting, `LGSF_DOCUMENT_STORAGE_BACKEND`.

### Running the GitHub path locally

To watch the whole journey without GitHub or AWS, a real scraper committing
to a repository and a reader importing from it, use the `git` backend:

```bash
LGSF_STORAGE_BACKEND=git LGSF_GIT_REMOTE=/tmp/lgsf-repos \
    python manage.py councillors --council KIR

git -C /tmp/lgsf-repos/KIR.git log --format='%h %s'
git -C /tmp/lgsf-repos/KIR.git log -1 --format='%(trailers)'
git -C /tmp/lgsf-repos/KIR.git ls-tree -r --name-only main
```

It runs the GitHub backend's own session code, so what lands on `main` is
what production would push: the same `<Type>/` layout, at most one commit
per run with the same subject and trailers, no commit when nothing changed,
no commit when the run failed, a fast-forward push. The differences are
that the remote is a bare repository on disk, made with `git init --bare`
the first time instead of through the GitHub API, and that the clone a run
writes into lives under `LGSF_GIT_WORKDIR` (default: `lgsf-git-work` in the
system temp directory). Point a reader at `/tmp/lgsf-repos/KIR.git` as it
would be pointed at `LGSF-Data/KIR`.

The commits are made with your own git configuration, so if you sign your
commits, each run that has something to commit will ask you to sign it.

## Running on AWS

Any scraper type's command can run on AWS instead of this machine, through
the same Step Function as the daily run, for that type only:

```bash
python manage.py minutes --aws --council KIR --wait
python manage.py councillors --aws --all-councils
```

`--wait` waits for the run and prints its report. The run refuses to start
while another execution covers one of the same councils; `--allow-overlap`
starts it anyway. The daily run does the types marked `daily` in
`lgsf/scraper_types.py`; see `cdk/README.md`.

### Sites that block AWS

Some councils refuse requests from AWS altogether (a 403 or a dropped
connection, whichever region it comes from) while the same scraper works
from a home connection. Set `run_on_aws = False` on that scraper and AWS
leaves it out of every run, scheduled or `--aws`. Run those from another
machine instead:

```bash
python manage.py councillors --all-councils --no-aws      # only the ones AWS skips
python manage.py councillors --all-councils --run-on-aws  # only what AWS runs
```

Without either flag a local run includes both.

## Working on a scraper

Install pre-commit hooks:
`$ pre-commit install`

## Finding something to work on

```
python manage.py minutes --list-missing      # councils with no minutes scraper
python manage.py minutes --list-disabled     # scrapers someone turned off
python manage.py metadata validate --service minutes
```

When you've picked a council, its existing `councillors` entry in
`scrapers/<CODE>/metadata.json` is the best clue to what its other services
need: a council on ModernGov for councillors is almost always on ModernGov for
meetings too, usually at the same base URL.

`metadata validate` checks that every council declaring a service has a
matching scraper file, that `base_url` is set, and that the scraper's base
class agrees with the recorded `cms_type`.

The dashboard publishes which councils failed in the last production run,
one feed per type: `api/failing/councillors.json`,
`api/failing/minutes.json`, and so on (the old `api/failing.json` is the
councillors list only). `--list-failing` prints that alongside what failed the
last time you ran each council locally. For a type with no production feed,
run the scrapers first: `manage.py <type> --all-councils --skip-documents`.

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
   `councillor_scraper_cmis`, `councillor_scraper_html`.

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
