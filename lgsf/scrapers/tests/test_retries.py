"""
Retrying and spacing out requests in ScraperBase.get.

Both are off by default and turned on per scraper type.
"""

import pytest
import requests
import wreq

from lgsf.scrapers import base
from lgsf.scrapers.base import ScraperBase


class FakeConsole:
    def __init__(self):
        self.lines = []

    def log(self, *args, **kwargs):
        self.lines.append(" ".join(str(a) for a in args))


class FakeResponse:
    def __init__(self, status_code=200, headers=None):
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class Scraper(ScraperBase):
    service_name = "test"
    scraper_object_type = "Test"


@pytest.fixture
def slept(monkeypatch):
    """Record sleeps instead of taking them."""
    calls = []
    monkeypatch.setattr(base.time, "sleep", calls.append)
    return calls


def make_scraper(answers, retries=3, request_interval=0):
    """A scraper whose requests get each of ``answers`` in turn."""
    scraper = Scraper.__new__(Scraper)
    scraper.options = {}
    scraper.console = FakeConsole()
    scraper.retries = retries
    scraper.request_interval = request_interval
    answers = list(answers)
    scraper.sent = []

    def send(url, extra_headers=None):
        scraper.sent.append(url)
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    scraper._send = send
    return scraper


def test_a_timeout_is_retried(slept):
    scraper = make_scraper(
        [wreq.exceptions.TimeoutError("timed out"), FakeResponse(200)]
    )

    assert scraper.get("https://a.example/x").status_code == 200
    assert len(scraper.sent) == 2
    assert slept == [5]


@pytest.mark.parametrize(
    "error",
    [
        wreq.exceptions.RequestError("connection closed before headers"),
        wreq.exceptions.DecodingError("IncompleteBody"),
        wreq.exceptions.ConnectionResetError("reset"),
        requests.exceptions.ChunkedEncodingError("truncated"),
    ],
)
def test_a_dropped_connection_is_retried(slept, error):
    """How a server hanging up part way through actually surfaces."""
    scraper = make_scraper([error, FakeResponse(200)])

    assert scraper.get("https://a.example/x").status_code == 200


def test_backoff_doubles(slept):
    scraper = make_scraper(
        [requests.ConnectionError("reset")] * 3 + [FakeResponse(200)]
    )

    scraper.get("https://a.example/x")

    assert slept == [5, 10, 20]


def test_gives_up_after_the_retries_and_raises_the_last_error(slept):
    scraper = make_scraper([wreq.exceptions.TimeoutError("timed out")] * 4)

    with pytest.raises(wreq.exceptions.TimeoutError):
        scraper.get("https://a.example/x")
    assert len(scraper.sent) == 4


def test_an_error_that_is_not_transient_is_not_retried(slept):
    scraper = make_scraper([ValueError("bad"), FakeResponse(200)])

    with pytest.raises(ValueError):
        scraper.get("https://a.example/x")
    assert len(scraper.sent) == 1


def test_a_404_is_an_answer_not_a_failure(slept):
    scraper = make_scraper([FakeResponse(404), FakeResponse(200)])

    with pytest.raises(requests.HTTPError):
        scraper.get("https://a.example/x")
    assert len(scraper.sent) == 1


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_try_again_later_statuses_are_retried(slept, status):
    scraper = make_scraper([FakeResponse(status), FakeResponse(200)])

    assert scraper.get("https://a.example/x").status_code == 200


def test_retry_after_is_honoured(slept):
    scraper = make_scraper(
        [FakeResponse(429, headers={"retry-after": "42"}), FakeResponse(200)]
    )

    scraper.get("https://a.example/x")

    assert slept == [42]


def test_retry_after_is_capped(slept):
    scraper = make_scraper(
        [FakeResponse(503, headers={"retry-after": "86400"}), FakeResponse(200)]
    )

    scraper.get("https://a.example/x")

    assert slept == [ScraperBase.max_retry_wait]


def test_out_of_retries_a_bad_status_raises_as_it_always_did(slept):
    scraper = make_scraper([FakeResponse(503)] * 4)

    with pytest.raises(requests.HTTPError):
        scraper.get("https://a.example/x")


def test_out_of_retries_a_bad_status_can_be_returned(slept):
    scraper = make_scraper([FakeResponse(503)] * 4)

    response = scraper.get("https://a.example/x", raise_for_status=False)

    assert response.status_code == 503


def test_retries_are_off_by_default(slept):
    """Lambda retries a failed invocation; most scrapers rely on that."""
    assert ScraperBase.retries == 0
    scraper = make_scraper(
        [wreq.exceptions.TimeoutError("timed out"), FakeResponse(200)], retries=0
    )

    with pytest.raises(wreq.exceptions.TimeoutError):
        scraper.get("https://a.example/x")
    assert slept == []


# ---- spacing requests out ----


@pytest.fixture
def clock(monkeypatch):
    """A clock that only moves when something sleeps."""
    now = [1000.0]
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(base.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(base.time, "sleep", sleep)
    monkeypatch.setattr(base, "_next_request_at", {})
    return slept


def test_requests_to_one_host_are_spaced_out(clock):
    scraper = make_scraper([FakeResponse(200)] * 3, request_interval=2)

    for _ in range(3):
        scraper.get("https://a.example/x")

    assert clock == [2, 2]


def test_other_hosts_are_not_held_up(clock):
    scraper = make_scraper([FakeResponse(200)] * 2, request_interval=2)

    scraper.get("https://a.example/x")
    scraper.get("https://b.example/x")

    assert clock == []


def test_the_interval_is_shared_between_scrapers(clock):
    """Several councils can share one site, and run in parallel."""
    first = make_scraper([FakeResponse(200)], request_interval=2)
    second = make_scraper([FakeResponse(200)], request_interval=2)

    first.get("https://shared.example/one")
    second.get("https://shared.example/two")

    assert clock == [2]


def test_retries_are_spaced_out_too(clock):
    scraper = make_scraper(
        [requests.ConnectionError("reset"), FakeResponse(200)], request_interval=10
    )

    scraper.get("https://a.example/x")

    # The backoff (5s) is less than the interval, so the throttle makes up
    # the difference before the retry goes out.
    assert clock == [5, 5]
