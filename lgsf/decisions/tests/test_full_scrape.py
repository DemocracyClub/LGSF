"""
What a long scrape needs: reading history in slices, not repeating work
across runs, and not losing work to a run that stops part way.

These drive the real ModernGov scraper against the real local storage
backends in a temporary directory, with only the HTTP layer faked.
"""

import datetime
import json
from pathlib import Path

import pytest
import wreq

from lgsf.conf import settings
from lgsf.decisions.scrapers import BaseDecisionsScraper, ModGovDecisionsScraper
from lgsf.scrapers.base import ScraperBase
from lgsf.storage.backends.base import StorageMode
from lgsf.storage.backends.local import LocalFilesystemStorage
from lgsf.storage.documents.local import LocalDocumentStorage

FIXTURES = Path(__file__).parent / "fixtures"
BASE_URL = "https://democracy.example.gov.uk"
TODAY = datetime.date.today()


def fixture(name):
    return (FIXTURES / name).read_text()


class FakeConsole:
    def log(self, *args, **kwargs):
        pass


class FakeResponse:
    def __init__(self, text="", status_code=200, headers=None, content=b""):
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise OSError(f"HTTP {self.status_code}")


def list_page(*ids, published="01/01/2020"):
    """A delegated decisions list page carrying these decision ids."""
    rows = "".join(
        f'<tr><td><a href="ieDecisionDetails.aspx?ID={i}">Decision {i}</a></td>'
        f"<td>{published}</td></tr>"
        for i in ids
    )
    return f"<table>{rows}</table>"


def detail_page(documents=()):
    links = "".join(f'<li><a href="{url}">Doc</a></li>' for url in documents)
    return (
        '<h2 class="mgSubTitleTxt">A decision</h2>'
        '<p><span class="mgLabel">Date of decision:</span> 01/01/2020</p>'
        f'<ul class="mgBulletList">{links}</ul>'
    )


@pytest.fixture(autouse=True)
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(tmp_path))
    return tmp_path


@pytest.fixture
def make_scraper(monkeypatch):
    """
    Build a ModernGov scraper on real local storage, answering requests
    from ``route(url)``, which returns a FakeResponse or raises.
    """

    def base_init(self, options, console):
        self.options = options
        self.console = console
        self.council_id = options["council"]
        self.base_url = BASE_URL
        self.storage_backend = LocalFilesystemStorage(
            council_code=self.council_id,
            scraper_object_type="Decisions",
            storage_mode=StorageMode.ACCUMULATE,
        )
        self.storage_session = self.storage_backend.start_session()

    monkeypatch.setattr(ScraperBase, "__init__", base_init)

    def build(route, **options):
        scraper = ModGovDecisionsScraper({"council": "TST", **options}, FakeConsole())
        scraper.requests = []

        def send(url, extra_headers=None):
            scraper.requests.append(url)
            return route(url)

        scraper._send = send
        scraper.request_interval = 0
        return scraper

    return build


def run(scraper):
    scraper.run(run_log=None)
    return scraper


def stored_index(data_dir):
    return json.loads((data_dir / "TST" / "Decisions" / "_index.json").read_text())


# ---- slicing the window ----


def test_slices_are_calendar_aligned_newest_first_and_cover_the_range(make_scraper):
    scraper = make_scraper(lambda url: None, since="2023-03-15")

    windows = scraper.windows()

    assert windows[-1].start == datetime.date(2023, 1, 1)
    assert windows[0].end == TODAY
    for newer, older in zip(windows, windows[1:]):
        assert older.last + datetime.timedelta(days=1) == newer.start
        assert older.start < newer.start


def test_a_slice_is_the_same_slice_on_another_day(make_scraper):
    """A slice is recorded as done by its key, so the key must not drift."""
    scraper = make_scraper(lambda url: None, since="2023-03-15")
    other = make_scraper(lambda url: None, since="2023-04-02")

    assert [w.key for w in scraper.windows()] == [w.key for w in other.windows()]


def test_the_default_window_is_still_about_a_year(make_scraper):
    scraper = make_scraper(lambda url: None)

    start = scraper.windows()[-1].start

    assert TODAY - start >= datetime.timedelta(days=365)
    assert TODAY - start < datetime.timedelta(days=365 + 31 * 6)


# ---- not repeating work ----


FIRST_HALF_2020 = "DR=01%2f01%2f2020-30%2f06%2f2020"


def history(ids=("1", "2")):
    """A council with some decisions published early in 2020, and no others."""

    def route(url):
        if "mgDelegatedDecisions" in url and FIRST_HALF_2020 in url:
            return FakeResponse(list_page(*ids))
        if "mgDelegatedDecisions" in url or "mgListOfficer" in url:
            return FakeResponse(list_page())
        if "ieDecisionDetails" in url:
            return FakeResponse(detail_page())
        raise AssertionError(url)

    return route


def test_a_backfill_stores_old_decisions(make_scraper, data_dir):
    run(make_scraper(history(), since="2019-06-01"))

    stored = sorted(p.name for p in (data_dir / "TST/Decisions/json").iterdir())
    assert stored == ["2020-01-01-1.json", "2020-01-01-2.json"]


def test_a_second_backfill_does_not_list_slices_already_read(make_scraper):
    run(make_scraper(history(), since="2019-06-01"))

    second = run(make_scraper(history(), since="2019-06-01"))

    assert second.windows_skipped >= 2
    assert not any(FIRST_HALF_2020 in u for u in second.requests)
    assert not any("ieDecisionDetails" in u for u in second.requests)


def test_recent_slices_are_always_listed(make_scraper, data_dir):
    """Decisions in them can still be amended, so they are never done."""
    run(make_scraper(history(), since="2019-06-01"))

    windows = stored_index(data_dir)["windows"]
    current = make_scraper(history()).windows()[0]

    assert current.key not in windows


def test_a_completed_slice_is_listed_again_eventually(make_scraper, data_dir):
    """Catches anything published late with an old publication date."""
    run(make_scraper(history(), since="2019-06-01"))
    index = stored_index(data_dir)
    long_ago = (TODAY - datetime.timedelta(days=60)).isoformat()
    for entry in index["windows"].values():
        entry["completed"] = long_ago
    (data_dir / "TST/Decisions/_index.json").write_text(json.dumps(index))

    again = run(make_scraper(history(), since="2019-06-01"))

    assert again.windows_skipped == 0
    # Listed again, but the decisions on it are settled: no page fetched.
    assert not any("ieDecisionDetails" in u for u in again.requests)


def test_a_slice_with_a_failed_decision_is_left_open(make_scraper, data_dir):
    def route(url):
        if "ID=2" in url:
            raise wreq.exceptions.ConnectionError("reset")
        return history()(url)

    scraper = make_scraper(route, since="2019-06-01")
    scraper.retries = 0
    run(scraper)

    assert scraper.failed_decisions == 1
    assert "2020-01-01/2020-06-30" not in stored_index(data_dir)["windows"]

    retry = run(make_scraper(history(), since="2019-06-01"))

    fetched = [u for u in retry.requests if "ieDecisionDetails" in u]
    assert fetched == [f"{BASE_URL}/ieDecisionDetails.aspx?ID=2"]


def test_a_slice_whose_list_failed_is_left_open(make_scraper, data_dir):
    def route(url):
        if "mgDelegatedDecisions" in url and FIRST_HALF_2020 in url:
            raise wreq.exceptions.TimeoutError("timed out")
        return history()(url)

    scraper = make_scraper(route, since="2019-06-01")
    scraper.retries = 0
    scraper.min_split_days = 1000
    run(scraper)

    assert scraper.windows_incomplete == 1
    assert "2020-01-01/2020-06-30" not in stored_index(data_dir)["windows"]


# ---- splitting a range the server can't manage ----


def test_a_range_that_times_out_is_split_until_it_works(make_scraper):
    scraper = make_scraper(lambda url: None)
    scraper.retries = 0
    ranges = []

    def route(url):
        start, end = url.split("DR=")[1].split("&")[0].split("-")
        start = datetime.datetime.strptime(start, "%d%%2f%m%%2f%Y").date()
        end = datetime.datetime.strptime(end, "%d%%2f%m%%2f%Y").date()
        ranges.append((start, end))
        if (end - start).days > 60:
            raise wreq.exceptions.TimeoutError("timed out")
        return FakeResponse(list_page(f"{start:%m%d}"))

    scraper._send = lambda url, extra_headers=None: route(url)

    rows, complete = scraper.read_list(
        datetime.date(2020, 1, 1), datetime.date(2020, 6, 30), False
    )

    assert complete
    covered = sorted(r for r in ranges if (r[1] - r[0]).days <= 60)
    assert covered[0][0] == datetime.date(2020, 1, 1)
    assert covered[-1][1] == datetime.date(2020, 6, 30)
    for before, after in zip(covered, covered[1:]):
        assert before[1] + datetime.timedelta(days=1) == after[0]
    assert len(rows) == len(covered)


def test_a_quick_404_is_a_missing_page_not_a_big_range(make_scraper):
    scraper = make_scraper(lambda url: FakeResponse(status_code=404))

    rows, complete = scraper.read_list(
        datetime.date(2020, 1, 1), datetime.date(2020, 6, 30), True
    )

    assert (rows, complete) == ([], True)
    assert len(scraper.requests) == 1


def test_a_slow_404_is_split(make_scraper, monkeypatch):
    """Kirklees answers eleven years with an error page after two minutes."""
    from lgsf.decisions import scrapers

    clock = iter(range(0, 10_000, 100))
    monkeypatch.setattr(scrapers.time, "monotonic", lambda: next(clock))
    scraper = make_scraper(lambda url: FakeResponse(status_code=404))
    scraper.min_split_days = 60

    _, complete = scraper.read_list(
        datetime.date(2020, 1, 1), datetime.date(2020, 6, 30), False
    )

    assert len(scraper.requests) == 3
    assert not complete


# ---- documents ----

DOC = f"{BASE_URL}/documents/s123/report.pdf"


def with_document(document_route):
    def route(url):
        if url == DOC:
            return document_route()
        if "ieDecisionDetails" in url:
            return FakeResponse(detail_page([DOC]))
        return history(ids=("1",))(url)

    return route


def test_a_document_already_on_disk_is_not_downloaded_again(make_scraper, data_dir):
    """
    Documents are written straight to the store but the index that records
    them waits for the session to end, so a run that stops part way leaves
    documents the index doesn't know about.
    """
    LocalDocumentStorage("TST").write("2020-01-01-1-s123.pdf", b"%PDF")

    def fail():
        raise AssertionError("should not be downloaded")

    scraper = run(make_scraper(with_document(fail), since="2019-06-01"))

    assert scraper.documents_skipped == 1
    record = json.loads((data_dir / "TST/Decisions/json/2020-01-01-1.json").read_text())
    assert record["documents"][0]["storage_key"] == "2020-01-01-1-s123.pdf"


def test_a_document_that_failed_keeps_its_decision_open(make_scraper, data_dir):
    def broken():
        raise wreq.exceptions.TimeoutError("timed out")

    first = make_scraper(with_document(broken), since="2019-06-01")
    first.retries = 0
    run(first)

    assert first.documents_failed == 1
    assert stored_index(data_dir)["decisions"][
        f"{BASE_URL}/ieDecisionDetails.aspx?ID=1"
    ]["incomplete"]

    second = run(
        make_scraper(
            with_document(lambda: FakeResponse(content=b"%PDF")), since="2019-06-01"
        )
    )

    assert second.documents_downloaded == 1
    entry = stored_index(data_dir)["decisions"][
        f"{BASE_URL}/ieDecisionDetails.aspx?ID=1"
    ]
    assert "incomplete" not in entry


def test_a_document_that_is_gone_does_not_keep_its_decision_open(
    make_scraper, data_dir
):
    scraper = run(
        make_scraper(
            with_document(lambda: FakeResponse(status_code=404)), since="2019-06-01"
        )
    )

    assert scraper.documents_unavailable == 1
    assert scraper.incomplete_decisions == 0
    record = json.loads((data_dir / "TST/Decisions/json/2020-01-01-1.json").read_text())
    assert record["documents"][0]["unavailable"] == 404


def test_skip_documents_does_not_settle_decisions_without_them(make_scraper, data_dir):
    """
    A --skip-documents backfill followed by a normal run has to go back for
    the documents, or settled decisions would never get them.
    """
    run(
        make_scraper(
            with_document(lambda: FakeResponse(content=b"%PDF")),
            since="2019-06-01",
            skip_documents=True,
        )
    )

    later = run(
        make_scraper(
            with_document(lambda: FakeResponse(content=b"%PDF")), since="2019-06-01"
        )
    )

    assert later.documents_downloaded == 1


# ---- not losing work ----


def test_progress_is_committed_as_the_run_goes(make_scraper, data_dir):
    ids = [str(i) for i in range(1, 8)]

    def route(url):
        if "ID=6" in url:
            raise KeyboardInterrupt
        return history(ids=ids)(url)

    scraper = make_scraper(route, since="2019-06-01")
    scraper.checkpoint_every = 2
    with pytest.raises(KeyboardInterrupt):
        run(scraper)

    stored = sorted(p.name for p in (data_dir / "TST/Decisions/json").iterdir())
    assert stored[:4] == [f"2020-01-01-{i}.json" for i in range(1, 5)]
    entries = stored_index(data_dir)["decisions"]
    assert all("file_name" in entries[u] for u in entries if "ID=1" in u)


def test_checkpoints_are_skipped_where_commits_are_expensive(make_scraper):
    scraper = make_scraper(history(), since="2019-06-01")
    scraper.checkpoint_every = 1
    ended = []
    scraper.storage_backend.supports_checkpoints = False
    original = scraper.storage_backend.end_session
    scraper.storage_backend.end_session = lambda *a, **k: (
        ended.append(a),
        original(*a, **k),
    )[1]

    run(scraper)

    assert len(ended) == 1


def test_decisions_scrape_politely_by_default():
    assert BaseDecisionsScraper.request_interval >= 1
    assert BaseDecisionsScraper.retries >= 1


# ---- a site that is down ----


def test_a_council_whose_site_is_down_is_given_up_on(make_scraper):
    """Rather than hours of timeouts against a site that isn't answering."""

    def route(url):
        raise wreq.exceptions.TimeoutError("timed out")

    scraper = make_scraper(route, since="2015-01-01")
    scraper.retries = 0
    run(scraper)

    assert scraper.given_up
    assert len(scraper.requests) == scraper.max_consecutive_failures


def test_giving_up_keeps_what_was_read(make_scraper, data_dir):
    ids = [str(i) for i in range(1, 10)]

    def route(url):
        if "ieDecisionDetails" in url and not url.endswith(("ID=1", "ID=2")):
            raise wreq.exceptions.TimeoutError("timed out")
        return history(ids=ids)(url)

    scraper = make_scraper(route, since="2019-06-01")
    scraper.retries = 0
    run(scraper)

    assert scraper.given_up
    fetched = [u for u in scraper.requests if "ieDecisionDetails" in u]
    assert len(fetched) == 2 + scraper.max_consecutive_failures
    stored = sorted(p.name for p in (data_dir / "TST/Decisions/json").iterdir())
    assert stored == ["2020-01-01-1.json", "2020-01-01-2.json"]


def test_occasional_failures_are_not_giving_up(make_scraper):
    ids = [str(i) for i in range(1, 13)]

    def route(url):
        if "ieDecisionDetails" in url and int(url.rsplit("=", 1)[1]) % 2:
            raise wreq.exceptions.TimeoutError("timed out")
        return history(ids=ids)(url)

    scraper = make_scraper(route, since="2019-06-01")
    scraper.retries = 0
    run(scraper)

    assert not scraper.given_up
    assert scraper.failed_decisions == 6


# ---- discovering where history starts ----


def requested_range(url):
    start, end = url.split("DR=")[1].split("&")[0].split("-")
    return (
        datetime.datetime.strptime(start, "%d%%2f%m%%2f%Y").date(),
        datetime.datetime.strptime(end, "%d%%2f%m%%2f%Y").date(),
    )


def published_from(first, later_gap=None):
    """
    A council whose oldest decision was published on ``first``, with
    decisions every month after that except during ``later_gap``.
    """

    def route(url):
        if "ieDecisionDetails" in url:
            return FakeResponse(detail_page())
        start, end = requested_range(url)
        start = max(start, first)
        if start > end or (later_gap and later_gap[0] <= start and end <= later_gap[1]):
            return FakeResponse(list_page())
        return FakeResponse(list_page(f"{start:%Y%m%d}", published=f"{start:%d/%m/%Y}"))

    return route


def test_discovers_the_oldest_publication_date(make_scraper):
    scraper = make_scraper(published_from(datetime.date(2013, 3, 14)))
    scraper.options["discover_since"] = True

    assert scraper.date_range[0] == datetime.date(2013, 3, 14)


def test_discovery_is_cheap(make_scraper):
    """Old ranges are empty, and walked forward a couple of years at a time."""
    scraper = make_scraper(published_from(datetime.date(2013, 3, 14)))

    scraper.discover_since()

    # One request for the last six months to check the site works, then
    # 2000 to 2013 in two-year chunks, two list pages each.
    assert len(scraper.requests) == 1 + 2 * 7


def test_a_gap_in_history_does_not_hide_older_decisions(make_scraper):
    scraper = make_scraper(
        published_from(
            datetime.date(2004, 6, 1),
            later_gap=(datetime.date(2006, 1, 1), datetime.date(2014, 12, 31)),
        )
    )

    assert scraper.discover_since() == datetime.date(2004, 6, 1)


def test_a_chunk_that_cannot_be_read_is_where_discovery_starts(make_scraper):
    """It might hold the oldest decisions: too early is cheap, too late loses."""

    def route(url):
        start, _ = requested_range(url)
        if start == datetime.date(2006, 1, 1):
            raise wreq.exceptions.TimeoutError("timed out")
        return published_from(datetime.date(2015, 1, 1))(url)

    scraper = make_scraper(route)
    scraper.retries = 0
    scraper.min_split_days = 10_000

    assert scraper.discover_since() == datetime.date(2006, 1, 1)


def test_a_council_with_no_decisions_uses_the_default_window(make_scraper):
    scraper = make_scraper(lambda url: FakeResponse(list_page()))
    scraper.options["discover_since"] = True

    start, _ = scraper.date_range

    assert TODAY - start == datetime.timedelta(days=365)


def test_discovery_is_remembered(make_scraper, data_dir):
    route = published_from(datetime.date(2013, 3, 14))
    run(make_scraper(route, discover_since=True))

    again = make_scraper(route, discover_since=True)
    again.windows()

    assert again.requests == []
    assert stored_index(data_dir)["discovered_since"] == "2013-03-14"
    first_slice = again.windows()[-1]
    assert first_slice.start == datetime.date(2013, 1, 1)


def test_a_discovery_cut_short_is_not_remembered(make_scraper, data_dir):
    def route(url):
        raise wreq.exceptions.TimeoutError("timed out")

    scraper = make_scraper(route, discover_since=True)
    scraper.retries = 0
    run(scraper)

    assert "discovered_since" not in stored_index(data_dir)


def test_a_site_that_errors_on_everything_is_not_searched(make_scraper, data_dir):
    """
    Enfield and Somerset redirect every page to mgError.aspx. Searching
    their history would only send the backfill back to 2000 to fail there.
    """

    def route(url):
        raise wreq.exceptions.TimeoutError("timed out")

    scraper = make_scraper(route)
    scraper.retries = 0
    scraper.min_split_days = 10_000

    assert scraper.discover_since() is None
    assert len(scraper.requests) == 2


def test_a_site_with_only_one_working_list_is_still_searched(make_scraper):
    def route(url):
        if "mgListOfficer" in url:
            raise wreq.exceptions.TimeoutError("timed out")
        return published_from(datetime.date(2013, 3, 14))(url)

    scraper = make_scraper(route)
    scraper.retries = 0
    scraper.min_split_days = 10_000
    scraper.max_consecutive_failures = 100

    # The officer list failing for a chunk is a chunk that can't be read,
    # so discovery stops at the first one: early, but never late.
    assert scraper.discover_since() == datetime.date(2000, 1, 1)
