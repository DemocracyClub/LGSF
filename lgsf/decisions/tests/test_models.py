"""
The decision record: what lands in JSON, and what comes back out.

`from_storage` is the mirror of `as_dict`, so a field added to one and not
the other is lost on the next run without anything failing.
"""

import json

from lgsf.decisions import DecisionBase


class FakeSession:
    def __init__(self, payload):
        self.payload = payload

    def open(self, filename):
        return self.payload


def make_decision():
    decision = DecisionBase(
        "https://example.gov.uk/ieDecisionDetails.aspx?ID=42",
        identifier="42",
        title="Award of Waste Contracts",
        date="2026-01-15",
        decision_maker="Director of Environment",
    )
    decision.status = "Recommendations Approved"
    decision.is_key_decision = True
    decision.is_subject_to_call_in = False
    decision.publication_date = "2026-01-17"
    decision.purpose = "To award the contract."
    decision.text = "The contract was awarded to the recommended bidder."
    decision.documents = [{"title": "Report", "url": "https://example.gov.uk/r.pdf"}]
    decision.source = {"url": "https://example.gov.uk/d", "etag": '"abc"'}
    return decision


def test_verbatim_fields_are_marked_raw():
    """Unnormalised source values carry a raw_ prefix, framework-wide."""
    out = make_decision().as_dict()

    assert out["raw_identifier"] == "42"
    assert out["raw_title"] == "Award of Waste Contracts"
    assert out["raw_date"] == "2026-01-15"
    assert out["raw_decision_maker"] == "Director of Environment"


def test_the_decision_text_is_stored_in_the_record():
    """The text is the point of a decision, and is small enough for git."""
    assert make_decision().as_dict()["text"] == (
        "The contract was awarded to the recommended bidder."
    )


def test_round_trips_through_storage():
    decision = make_decision()
    session = FakeSession(decision.as_json())

    loaded = DecisionBase.from_storage("42.json", session)

    assert loaded == decision
    assert loaded.as_dict() == decision.as_dict()


def test_a_decision_with_nothing_filled_in_still_serialises():
    """
    The init=False fields are genuinely unset when a scraper raises before
    assigning them, and error reporting must not blow up on that.
    """
    bare = DecisionBase(
        "https://example.gov.uk/d", identifier="1", title="T", date="2026-01-01"
    )

    out = json.loads(bare.as_json())

    assert out["text"] is None
    assert out["documents"] == []


def test_file_name_is_stable_and_safe():
    assert make_decision().as_file_name() == "2026-01-15-42"


def test_identity_is_the_identifier_not_the_content():
    """Two scrapes of the same decision are the same decision."""
    first = make_decision()
    second = make_decision()
    second.title = "Changed in a later run"

    assert first == second
    assert len({first, second}) == 1
