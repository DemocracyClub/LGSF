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
        "category": self.categorise_document(title, date),
    }
]
meeting.minutes_published = True
```

Pass the meeting's date to `categorise_document`: a minutes document whose
title names another date (the previous meeting's, up for approval) is then
`"previous_minutes"`, not this meeting's `"minutes"`.

`identifier` must be stable across runs — an id from the source system, not a
row number. It ends up in the filename, so an unstable one silently creates
duplicates.

## What a meeting's JSON holds

Every scraper writes the fields above. These say more about the meeting,
and are filled where the source gives them: ModernGov gives all of them,
CMIS and custom scrapers fill what they can and leave the rest empty
(`{}`, `[]` or `null`).

| Field | What it is | ModernGov source |
| --- | --- | --- |
| `times` | `scheduled`, `actual_start`, `actual_finish`, `planned_finish`, as given, e.g. `"7.00 pm"`. No dates: an actual finish can be after midnight | `meetingtime`, `meetingactualstarttime`, `meetingactualfinishtime`, `meetingplannedfinishtime` |
| `published` | `agenda`, `decisions`, `minutes`, each true or false | `agendapublished`, `decisionpublished`, `minutepublished` |
| `is_webcast` | Whether the meeting is webcast | `iswebcast` |
| `install_url` | The base URL of the system the meeting came from. Councils sharing one install list the same meetings: the same install and `raw_identifier` is the same meeting | the scraper's base URL |
| `agenda_items` | In the source's order. Each has `id`, `number`, `sub_number`, `display_number` (the minute reference, e.g. Stroud's `CL.025a`), `minutes_number`, `minutes_sub_number`, `title`, `agenda_html`, `minutes_html`, `is_decision`, `is_decision_published`, `is_minute_published`, `is_call_in`, `is_restricted`, `is_public_can_speak`, `declaration_count`, `refer_to_committee`, `preferred_date`, and `document_ids`, the `attachment_id`s of its documents | `agendaitem` and its children |
| `attendance` | Each person listed, with `member_id`, `member_type`, `name`, `role`, `attendance` (e.g. `"Present, as expected"`, `"Apologies"`), `party`, `ward`, `key_posts`, `url`, `reason` and `substituted_by`. `member_id` is the councillors scraper's id, so attendance joins to councillors | `attendee` attributes |
| `declarations` | Declarations of interest, each with `member_name`, `member_id`, `item_ref`, `details`, `nature`, `action`. ModernGov gives only a `declaration_count` per agenda item, so this is `[]` there | (CMIS only) |

**CMIS** fills `attendance` and `declarations` from the meeting page's
Attendance and Declarations tabs. `attendance` is CMIS's own heading:
`"Attended - Committee Members"`, `"Attended - Other Members"`,
`"Apologies"` or `"Absent"`, with the reason given and any substitute.
`member_id` comes from the person's `.../ViewCMIS_Person/.../id/<id>/`
link, the same id the CMIS councillors scraper records.

Each document in `documents` also gives, from ModernGov, the
`agenda_item_id` it hangs from, `owner_title`, `publication_date` (ISO),
`is_restricted` and `is_decision_attachment`. A meeting's own minutes are
often not a document at all but the `minutes_html` of its items.

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
touching the metadata. The store is shared with decisions. Each file is named
after its content: the SHA-256 of its bytes, then an extension worked out from
the bytes. So a report linked from a meeting and from a decision is stored
once, though each downloads it.

A document already in the store is not downloaded again, so a second run over
the same window costs almost nothing. `--skip-documents` skips downloading
entirely, which is what you want while iterating on parsing: a single council
can be hundreds of megabytes.

A document that answers 404 or 410, or 200 with no body, is recorded on the
meeting as `unavailable` (the status code, or `"empty"`) and counted as that in
the run report, not as a failed download. It isn't recorded in the index, so
a later run that reads the meeting again asks for it again.

## Checkpoints

A run commits what it has stored every `checkpoint_every` (50) meetings
saved, where the storage backend can do that cheaply (`supports_checkpoints`:
local and S3 yes, GitHub no, since each commit there is a pull request). An
interrupted run loses at most the meetings since its last checkpoint.

Their documents are already in the document store, but only a saved index
records which link each came from, so the next run downloads those again. It
finds them already stored and doesn't write them a second time.

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
