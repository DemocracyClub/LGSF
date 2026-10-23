"""
What the GitHub backend does when the API misbehaves.

The git plumbing is tested elsewhere against a bare repository; these cover
the few API calls left (does the repository exist, is it empty, create it)
and the request helper under them, with scripted responses and no network.
"""

import pytest
import requests

from lgsf.storage.backends.github import (
    GitHubAppAuthenticator,
    GitHubRateLimitError,
    _GitHubSession,
    github_api_request,
)


class FakeResponse:
    def __init__(self, status_code, headers=None, body=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self._body = body
        self.text = text

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


class FakeApi:
    """Answers each request with the next scripted response, recording the calls."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url))
        answer = self.responses.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.fixture
def sleeps():
    waited = []
    return waited, waited.append


def test_a_5xx_is_retried_with_growing_waits(sleeps):
    waited, sleep = sleeps
    api = FakeApi(
        FakeResponse(502), FakeResponse(503), FakeResponse(200, body={"ok": 1})
    )

    response = github_api_request(api, "GET", "https://api.github.com/x", sleep=sleep)

    assert response.status_code == 200
    assert waited == [1, 2]
    assert len(api.calls) == 3


def test_the_last_failing_response_is_returned_for_the_caller_to_raise_on(sleeps):
    waited, sleep = sleeps
    api = FakeApi(*[FakeResponse(500)] * 4)

    response = github_api_request(api, "GET", "https://api.github.com/x", sleep=sleep)

    assert response.status_code == 500
    assert waited == [1, 2, 4]
    with pytest.raises(requests.exceptions.HTTPError):
        response.raise_for_status()


def test_a_connection_error_is_retried_too(sleeps):
    waited, sleep = sleeps
    api = FakeApi(requests.exceptions.ConnectionError("reset"), FakeResponse(200))

    assert github_api_request(api, "GET", "u", sleep=sleep).status_code == 200
    assert waited == [1]


def test_a_connection_error_on_the_last_attempt_propagates(sleeps):
    waited, sleep = sleeps
    api = FakeApi(*[requests.exceptions.ConnectionError("reset")] * 4)

    with pytest.raises(requests.exceptions.ConnectionError):
        github_api_request(api, "GET", "u", sleep=sleep)
    assert waited == [1, 2, 4]


def test_an_exhausted_primary_rate_limit_is_raised_not_retried(sleeps):
    waited, sleep = sleeps
    api = FakeApi(
        FakeResponse(
            403,
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1761300000"},
        )
    )

    with pytest.raises(GitHubRateLimitError) as raised:
        github_api_request(api, "GET", "u", sleep=sleep)

    assert raised.value.reset_time == 1761300000
    assert waited == []
    assert len(api.calls) == 1


def test_a_short_secondary_rate_limit_is_waited_out_once(sleeps):
    waited, sleep = sleeps
    api = FakeApi(FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200))

    assert github_api_request(api, "POST", "u", sleep=sleep).status_code == 200
    assert waited == [7]


def test_a_long_or_repeated_secondary_rate_limit_is_raised(sleeps):
    waited, sleep = sleeps
    with pytest.raises(GitHubRateLimitError):
        github_api_request(
            FakeApi(FakeResponse(403, headers={"Retry-After": "3600"})),
            "GET",
            "u",
            sleep=sleep,
        )
    with pytest.raises(GitHubRateLimitError):
        github_api_request(
            FakeApi(
                FakeResponse(429, headers={"Retry-After": "5"}),
                FakeResponse(429, headers={"Retry-After": "5"}),
            ),
            "GET",
            "u",
            sleep=sleep,
        )
    assert waited == [5]


def test_a_plain_403_is_not_a_rate_limit(sleeps):
    waited, sleep = sleeps
    response = github_api_request(
        FakeApi(FakeResponse(403, text="Forbidden")), "GET", "u", sleep=sleep
    )
    assert response.status_code == 403


def test_other_4xx_are_returned_as_answers(sleeps):
    waited, sleep = sleeps
    assert (
        github_api_request(
            FakeApi(FakeResponse(404)), "GET", "u", sleep=sleep
        ).status_code
        == 404
    )
    assert waited == []


# --- the session's calls ---


def session_with(*responses):
    session = _GitHubSession(
        organization="LGSF-Data",
        github_token="t",
        council_code="ABC",
        scraper_object_type="Councillors",
    )
    session.session = FakeApi(*responses)
    session._sleep = lambda seconds: None
    return session


def test_repo_exists_reads_a_404_as_no_and_a_200_as_yes():
    assert session_with(FakeResponse(404))._repo_exists_on_github() is False
    assert session_with(FakeResponse(200))._repo_exists_on_github() is True


def test_repo_exists_raises_rather_than_guess_when_the_api_is_down():
    session = session_with(*[FakeResponse(503)] * 4)
    with pytest.raises(requests.exceptions.HTTPError):
        session._repo_exists_on_github()


def test_a_rate_limit_on_any_call_reaches_the_caller():
    limited = FakeResponse(403, headers={"X-RateLimit-Remaining": "0"})
    with pytest.raises(GitHubRateLimitError):
        session_with(limited)._repo_exists_on_github()
    with pytest.raises(GitHubRateLimitError):
        session_with(limited)._check_if_repo_is_empty()
    with pytest.raises(GitHubRateLimitError):
        session_with(limited)._create_repository()


def test_creating_a_repository_then_makes_its_first_commit():
    session = session_with(FakeResponse(201), FakeResponse(201))

    session._create_repository()

    assert [m for m, _ in session.session.calls] == ["POST", "PUT"]
    assert session.session.calls[1][1].endswith("/contents/README.md")


def test_a_repository_that_already_exists_is_not_an_error():
    session = session_with(FakeResponse(422))
    session._create_repository()
    assert len(session.session.calls) == 1


# --- the App token ---


def test_the_installation_token_request_is_retried(monkeypatch):
    auth = GitHubAppAuthenticator(app_id="1", installation_id="2", private_key="k")
    monkeypatch.setattr(auth, "_generate_jwt", lambda: "jwt")
    auth._http = FakeApi(
        FakeResponse(502), FakeResponse(201, body={"token": "ghs_abc"})
    )
    monkeypatch.setattr("lgsf.storage.backends.github.time.sleep", lambda s: None)

    token, expires_at = auth._request_installation_token()

    assert token == "ghs_abc"
    assert len(auth._http.calls) == 2
