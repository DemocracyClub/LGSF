"""
Councils that share one website: each scraper leaves out what is only the
other council's (lgsf/shared_sites.py), and keeps what is joint.
"""

import datetime
import importlib.util
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from lgsf.decisions.models import DecisionBase
from lgsf.shared_sites import only_other_councils

SCRAPERS = Path(__file__).resolve().parents[2] / "scrapers"


def load_scraper(directory, name):
    spec = importlib.util.spec_from_file_location(
        f"{directory}.{name}", SCRAPERS / directory / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Scraper


@pytest.mark.parametrize(
    "text,other_councils",
    [
        # Committees, as Adur and Worthing's site names them
        ("Worthing Planning Committee", True),
        ("Archived Worthing Executive Leader", True),
        ("Joint Strategic Sub-Committee (Worthing)", True),
        ("Joint Strategic Sub-Committee (Adur)", False),
        ("Joint Strategic Committee", False),
        ("Adur Council", False),
        ("Officer Executive Decision Notices - pre july 2019", False),
        # Decision makers
        ("Worthing Leader", True),
        ("10/03/2020 - Worthing Cabinet", True),
        ("Worthing Cabinet Member for Finance and Resources, Chief Executive", True),
        (
            "Adur Cabinet Member for Housing, Worthing Cabinet Member for Housing",
            False,
        ),
        ("Service Director for Place & Economy (CLT)", False),
        ("", False),
        (None, False),
    ],
)
def test_only_other_councils(text, other_councils):
    assert only_other_councils(text, "Adur", ["Worthing"]) is other_councils


def test_a_council_named_inside_a_word_is_not_named():
    assert not only_other_councils("Adurworthington Panel", "Adur", ["Worthing"])


class FakeConsole:
    def log(self, *args, **kwargs):
        pass


def committees_xml(*titles):
    rows = "".join(
        f"<committee><committeeid>{i}</committeeid>"
        f"<committeetitle>{title}</committeetitle></committee>"
        for i, title in enumerate(titles, 1)
    )
    return BeautifulSoup(f"<committees>{rows}</committees>", features="xml")


@pytest.mark.parametrize(
    "directory,kept",
    [
        ("ADU-adur", ["Adur Planning Committee", "Joint Strategic Committee"]),
        ("WOT-worthing", ["Worthing Cabinet", "Joint Strategic Committee"]),
    ],
)
def test_each_council_keeps_its_own_and_the_joint_committees(directory, kept):
    Scraper = load_scraper(directory, "minutes")
    scraper = Scraper.__new__(Scraper)
    scraper.base_url = "https://democracy.adur-worthing.gov.uk"
    soup = committees_xml(
        "Adur Planning Committee",
        "Worthing Cabinet",
        "Joint Strategic Committee",
    )
    scraper.get_meetings = lambda: soup.find_all("committee")
    saved = []
    scraper.save_committees = saved.extend
    fetched = []
    scraper.get_meeting_stubs = lambda committee_id: fetched.append(committee_id) or []
    scraper.save_index = lambda: None
    scraper.finalize_storage = lambda run_log: None
    scraper.report = lambda: None

    scraper.run(run_log=object())

    assert [c.find("committeetitle").text for c in saved] == kept
    assert len(fetched) == 2, "no meetings are fetched for a left-out committee"


def make_decisions_scraper(directory, makers):
    Scraper = load_scraper(directory, "decisions")
    scraper = Scraper.__new__(Scraper)
    scraper.console = FakeConsole()
    for counter in (
        "unchanged_decisions",
        "settled_decisions",
        "failed_decisions",
        "fetched_decisions",
        "excluded_decisions",
        "consecutive_failures",
        "checkpoint_every",
        "incomplete_decisions",
    ):
        setattr(scraper, counter, 0)
    scraper.decisions = set()
    scraper.index = {"decisions": {}}
    scraper.settled_after_months = 3
    old = (datetime.date.today() - datetime.timedelta(days=365)).isoformat()
    scraper.get_decisions = lambda: [
        {"url": url, "published_date": old} for url in makers
    ]

    def get_single_decision(row):
        decision = DecisionBase(
            row["url"],
            identifier=row["url"],
            title="A decision",
            date=old,
            decision_maker=makers[row["url"]],
        )
        scraper.decisions.add(decision)
        return decision

    scraper.get_single_decision = get_single_decision
    scraper.processed = []
    scraper.process_decision = lambda decision, raw: scraper.processed.append(
        decision.url
    )
    scraper.save_index = lambda: None
    scraper.finalize_storage = lambda run_log: None
    scraper.report = lambda: None
    return scraper


MAKERS = {
    "adur": "Adur Leader",
    "worthing": "Worthing Leader",
    "both": "Adur Leader, Worthing Leader",
    "officer": "Chief Executive",
    "joint": "Joint Strategic Committee",
}


def test_adur_leaves_out_worthing_decisions_before_saving_them():
    scraper = make_decisions_scraper("ADU-adur", MAKERS)

    scraper.run(run_log=object())

    assert scraper.processed == ["adur", "both", "officer", "joint"]
    assert "worthing" not in {d.url for d in scraper.decisions}
    assert scraper.excluded_decisions == 1
    assert scraper.index["decisions"]["worthing"] == {"excluded": True}


def test_worthing_leaves_out_adur_decisions():
    scraper = make_decisions_scraper("WOT-worthing", MAKERS)

    scraper.run(run_log=object())

    assert scraper.processed == ["worthing", "both", "officer", "joint"]


def test_a_settled_left_out_decision_is_not_fetched_again():
    scraper = make_decisions_scraper("ADU-adur", MAKERS)
    scraper.run(run_log=object())
    row = {"url": "worthing", "published_date": "2020-01-01"}

    assert scraper.is_settled(row)


def test_a_recent_left_out_decision_is_fetched_again():
    scraper = make_decisions_scraper("ADU-adur", MAKERS)
    scraper.run(run_log=object())
    row = {"url": "worthing", "published_date": datetime.date.today().isoformat()}

    assert not scraper.is_settled(row)
