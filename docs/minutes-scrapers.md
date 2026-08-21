# Minutes scrapers

The scraper classes for committee meetings and their documents. For running
them see [running-scrapers.md](running-scrapers.md).

A minutes scraper produces a set of `MeetingBase` objects, each with the
documents linked from that meeting. Two CMS-specific classes cover most
councils, and `CustomHTMLMinutesScraper` covers the rest.

All of them read `base_url` from the council's `metadata.json`
(`services.minutes.base_url`), not from the class.

**Starting a council from scratch?** Look at its `councillors` entry in
`metadata.json` first. A council's services nearly always share a host, so if
councillors is ModernGov or CMIS then minutes probably is too, and the job is a
subclass plus a config block rather than a hand-written scraper.

### `ModGovMinutesScraper`

For ModernGov sites — the same ones `ModGovCouncillorScraper` handles. Usually
all you need:

```python
from lgsf.minutes.scrapers import ModGovMinutesScraper


class Scraper(ModGovMinutesScraper):
    pass
```

`base_url` must be the directory containing `mgWebService.asmx`, e.g.
`https://democracy.kirklees.gov.uk`, not a page within the site. Test it by
requesting `{base_url}/mgWebService.asmx/GetCommittees` — you should get XML
listing committees.

### `CMISMinutesScraper`

For CMIS sites. `base_url` is the meetings page, e.g.
`https://birmingham.cmis.uk.com/birmingham/Meetings.aspx`. The page name
varies between installs, so if `Meetings.aspx` 404s try
`MeetingsCalendar.aspx` or `CalendarofMeetings.aspx`.

CMIS pages are slow — a month of the calendar takes around 40 seconds to
render — so these scrapers use a longer timeout and a run takes minutes rather
than seconds.

### `CustomHTMLMinutesScraper`

For everything else. It supplies `self.get_page(url)`, which fetches and parses
a page. Scaffold it with the `minutes_scraper_custom` template and implement
two methods:

`get_meetings()` returns an iterable of whatever represents one meeting on
that site — a BeautifulSoup `Tag`, a dict from a JSON API, anything. Use
`self.date_range` to skip meetings outside the scrape window.

`get_single_meeting(meeting_data)` takes one of those and returns a meeting
built with `self.add_meeting()`.

```python
meeting = self.add_meeting(
    url,
    identifier=identifier,
    title=title,
    committee=committee,
    date=date,
    committee_id=committee_id,
)
meeting.status = "Confirmed"
meeting.location = "Council Chamber"
meeting.documents = [
    {
        "title": "Minutes of the previous meeting",
        "url": "https://.../minutes.pdf",
        "attachment_id": "12345",
        "category": self.categorise_document(title),
    }
]
meeting.minutes_published = True
```

`identifier` must be stable across runs — an id from the source system, not a
row number. It ends up in the filename, so an unstable one silently creates
duplicates.

## The scrape window

Meetings are scraped over a rolling window around today, set by two class
attributes:

```python
weeks_back = 4
weeks_forward = 1
```

Minutes are usually published days or weeks after a meeting, which is why the
window looks backwards: revisiting a meeting is how its minutes get picked up
once they appear. Override either attribute on a council's scraper if its
publishing schedule needs it.

## Skipping meetings

`get_single_meeting()` must return a meeting. Where it can't — a header row in
a table, or a meeting belonging to a neighbouring council that shares a site —
raise `SkipMeetingException` and the loop moves on:

```python
from lgsf.minutes.exceptions import SkipMeetingException
```

## Documents, and where they go

Metadata and document files are stored separately.

The JSON for a meeting goes to `data/<COUNCIL>/Minutes/json/` and is meant for
version control. Each document it links records a `content_hash`, a
`storage_key` and the backend that holds it — but not a resolved URL, which
would be specific to whoever ran the scrape.

The files themselves go to `data/<COUNCIL>/documents/` via a pluggable
document-storage backend, so they can be pointed at an object store without
touching the metadata.

A document already in the store is not downloaded again, so a second run over
the same window costs almost nothing. `--skip-documents` skips downloading
entirely, which is what you want while iterating on parsing: a single council
can be hundreds of megabytes.

## When a scraper runs but finds no meetings

Either a selector has stopped matching, or the council genuinely held no
meetings in the window. Both are common, and the run looks identical.

Ask the council's own calendar or API for the same date range. If it shows
meetings the scraper missed, it's a real bug; if it shows none, widen
`weeks_back` before concluding anything.

## Known limitations

These are framework-wide, not bugs in any one council's scraper:

- **ModernGov sends no HTTP validators.** No `ETag` or `Last-Modified` on
  meeting detail, so conditional requests never produce a 304 and every run
  re-fetches every meeting's XML. Comparing fetched XML against the stored copy
  would be the way in.
- **Webcast URLs are ignored.** ModernGov publishes them in a `mgMeetingMedia`
  script block that nothing currently reads.
- **Documents are categorised by keyword.** `categorise_document` matches
  "minute" in the title, which mislabels the occasional report. Matching
  "printed minutes" / "printed draft minutes" would be tighter.
- **CMIS costs one calendar render per month** in the window, around 40 seconds
  each.
