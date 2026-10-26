"""
The doorbell: one SNS message per push to a council's main.

The push itself runs the real git plumbing against a bare repository, as
in test_github_layout; only SNS is faked.
"""

import json
from pathlib import Path

import pytest

from lgsf.storage import doorbell
from lgsf.storage.backends.git import LocalGitStorage
from lgsf.storage.backends.github import GitHubStorage
from lgsf.storage.tests.test_github_layout import (
    MESSAGE,
    git,
    make_session,
    seed_main,
)

pytestmark = pytest.mark.usefixtures("isolated_git")

TOPIC = "arn:aws:sns:eu-west-2:123456789012:lgsf-development-topic-data-commits"


class FakeSNS:
    def __init__(self, fail=False):
        self.published = []
        self.fail = fail

    def publish(self, **kwargs):
        if self.fail:
            raise RuntimeError("SNS is down")
        self.published.append(kwargs)

    @property
    def messages(self):
        return [json.loads(p["Message"]) for p in self.published]


@pytest.fixture
def sns(monkeypatch):
    fake = FakeSNS()
    monkeypatch.setattr(doorbell, "get_client", lambda region: fake)
    monkeypatch.setenv(doorbell.TOPIC_ARN_VARIABLE, TOPIC)
    return fake


def storage_for(session, cls=GitHubStorage):
    """A storage with session active, without GitHubStorage's App set-up."""
    store = object.__new__(cls)
    store.council_code = session.council_code
    store._active = session
    return store


def push(bare, tmp_path, monkeypatch, files, cls=GitHubStorage):
    session = make_session(bare, tmp_path, monkeypatch)
    for name, content in files.items():
        session.write(Path(name), content)
    store = storage_for(session, cls)
    try:
        return store._end_session(session, MESSAGE)
    finally:
        session._cleanup_local_repo()


def main_sha(bare):
    return git("rev-parse", "main", cwd=bare).strip()


def test_a_push_rings_once_with_the_council_and_where_main_now_is(
    sns, bare_repo, tmp_path, monkeypatch
):
    result = push(bare_repo, tmp_path, monkeypatch, {"json/a.json": "{}"})

    assert result["commit"] == main_sha(bare_repo)
    assert sns.messages == [
        {
            "repository": "ABC",
            "after": main_sha(bare_repo),
            "scraper_type": "Councillors",
            "run_id": sns.messages[0]["run_id"],
        }
    ]
    assert sns.published[0]["TopicArn"] == TOPIC


def test_the_message_names_the_repository_not_the_code_as_given(
    sns, bare_repo, tmp_path, monkeypatch
):
    session = make_session(bare_repo, tmp_path, monkeypatch)
    session.council_code = "abc"  # the repository is still ABC
    session.write(Path("json/a.json"), "{}")
    try:
        storage_for(session)._end_session(session, MESSAGE)
    finally:
        session._cleanup_local_repo()

    assert sns.messages[0]["repository"] == "ABC"


def test_nothing_to_push_rings_nothing(sns, bare_repo, tmp_path, monkeypatch):
    push(bare_repo, tmp_path, monkeypatch, {"json/a.json": "{}"})
    push(bare_repo, tmp_path, monkeypatch, {"json/a.json": "{}"})

    assert len(sns.messages) == 1


def test_a_rejected_push_rings_nothing(sns, bare_repo, tmp_path, monkeypatch):
    session = make_session(bare_repo, tmp_path, monkeypatch)
    session.write(Path("json/a.json"), "{}")
    session._ensure_local_repo()
    # The clone is now stale: another run lands on main before this one pushes
    monkeypatch.setattr(session, "_ensure_local_repo", lambda: None)
    seed_main(bare_repo, tmp_path, {"Minutes/json/m.json": "{}"})

    with pytest.raises(RuntimeError, match="moved while"):
        storage_for(session)._end_session(session, MESSAGE)

    assert sns.messages == []


def test_a_failed_ring_doesnt_fail_the_push(bare_repo, tmp_path, monkeypatch):
    monkeypatch.setattr(doorbell, "get_client", lambda region: FakeSNS(fail=True))
    monkeypatch.setenv(doorbell.TOPIC_ARN_VARIABLE, TOPIC)

    result = push(bare_repo, tmp_path, monkeypatch, {"json/a.json": "{}"})

    assert result["commit"] == main_sha(bare_repo)


def test_the_local_git_backend_never_rings(sns, bare_repo, tmp_path, monkeypatch):
    push(bare_repo, tmp_path, monkeypatch, {"json/a.json": "{}"}, cls=LocalGitStorage)

    assert sns.messages == []


def test_without_a_topic_nothing_is_sent(sns, monkeypatch):
    monkeypatch.delenv(doorbell.TOPIC_ARN_VARIABLE)

    assert doorbell.ring("ABC", "abc123") is False
    assert sns.messages == []


def test_extras_that_are_none_are_left_out(sns):
    assert doorbell.ring("ABC", None, run_id=None, scraper_type="Minutes")

    assert sns.messages == [{"repository": "ABC", "scraper_type": "Minutes"}]


def test_the_client_is_for_the_topics_region(monkeypatch):
    regions = []
    monkeypatch.setattr(
        doorbell, "get_client", lambda region: regions.append(region) or FakeSNS()
    )

    doorbell.ring("ABC", "abc", topic_arn=TOPIC)

    assert regions == ["eu-west-2"]
