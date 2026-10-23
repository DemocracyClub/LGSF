import json
from dataclasses import dataclass, field
from pathlib import Path

from slugify import slugify


#: Fields written only when the source gives them.
EXTRA_FIELDS = (
    "decided_at_meeting",
    "issue_id",
    "effective_from",
    "reasons",
    "alternative_options",
    "report_author",
    "interests_declared",
    "call_in_deadline",
    "call_in_count",
    "wards_affected",
    "install_url",
)


@dataclass
class DecisionBase:
    url: str
    identifier: str
    title: str
    date: str
    decision_maker: str = None
    # Fields the scraper fills in after construction. Kept out of hash and
    # equality: two scrapes of the same decision are the same decision.
    status: str = field(init=False, hash=False, compare=False)
    is_key_decision: bool = field(init=False, hash=False, compare=False)
    is_subject_to_call_in: bool = field(init=False, hash=False, compare=False)
    #: True when the source listed this among its officer decisions. An
    #: officer decision is one delegated to an officer rather than taken by
    #: a committee or a member, so these are a subset of the delegated
    #: decisions, not a separate kind of record.
    is_officer_decision: bool = field(init=False, hash=False, compare=False)
    publication_date: str = field(init=False, hash=False, compare=False)
    purpose: str = field(init=False, hash=False, compare=False)
    #: The decision text itself, stored in the metadata store rather than the
    #: document store: it is small, and it is the point of the record.
    text: str = field(init=False, hash=False, compare=False)
    documents: list = field(init=False, hash=False, compare=False)
    # Where this decision was fetched from, plus any HTTP validators the
    # server gave us, so the next run can ask "has this changed?"
    source: dict = field(init=False, hash=False, compare=False)

    # More from the decision page, where it says it. See
    # docs/decision-scrapers.md.
    #: The meeting the decision was taken at: {"meeting_id", "committee_id",
    #: "url", "title"}, so a decision joins to the meeting record
    decided_at_meeting: dict = field(init=False, hash=False, compare=False)
    #: The source's issue the decision belongs to (ModernGov's IId): one
    #: issue can be decided in steps, at several meetings
    issue_id: str = field(init=False, hash=False, compare=False)
    effective_from: str = field(init=False, hash=False, compare=False)
    reasons: str = field(init=False, hash=False, compare=False)
    alternative_options: str = field(init=False, hash=False, compare=False)
    report_author: str = field(init=False, hash=False, compare=False)
    #: As written, often "None."
    interests_declared: str = field(init=False, hash=False, compare=False)
    #: While a decision can still be called in
    call_in_deadline: str = field(init=False, hash=False, compare=False)
    call_in_count: int = field(init=False, hash=False, compare=False)
    wards_affected: str = field(init=False, hash=False, compare=False)
    #: The base URL of the system the decision was read from; see meetings
    install_url: str = field(init=False, hash=False, compare=False)

    def __repr__(self):
        return "<Decision: {} on {}>".format(self.title, self.date)

    def __hash__(self):
        return hash(self.identifier)

    def __eq__(self, other):
        return (
            issubclass(type(other), DecisionBase)
            and self.identifier == other.identifier
        )

    def as_file_name(self):
        return f"{slugify(self.date)}-{slugify(self.identifier)}"

    @classmethod
    def from_storage(cls, filename: Path, session):
        """Load a decision from storage. Mirror of as_dict()."""
        data = json.loads(session.open(filename))

        extras = {}
        for key in (
            "status",
            "is_key_decision",
            "is_subject_to_call_in",
            "is_officer_decision",
            "publication_date",
            "purpose",
            "text",
            "documents",
            "source",
            *EXTRA_FIELDS,
        ):
            extras[key] = data.pop(key, None)

        for k in list(data.keys()):
            if k.startswith("raw_"):
                data[k[4:]] = data.pop(k)

        decision = cls(**data)
        for key, value in extras.items():
            if value is not None:
                setattr(decision, key, value)
        return decision

    def as_dict(self):
        out = {
            "url": self.url,
            "status": getattr(self, "status", None),
            "is_key_decision": getattr(self, "is_key_decision", None),
            "is_subject_to_call_in": getattr(self, "is_subject_to_call_in", None),
            "is_officer_decision": getattr(self, "is_officer_decision", None),
            "publication_date": getattr(self, "publication_date", None),
            "purpose": getattr(self, "purpose", None),
            "text": getattr(self, "text", None),
            "documents": getattr(self, "documents", []),
            "source": getattr(self, "source", {}),
            "decided_at_meeting": getattr(self, "decided_at_meeting", None),
            "issue_id": getattr(self, "issue_id", None),
            "effective_from": getattr(self, "effective_from", None),
            "reasons": getattr(self, "reasons", None),
            "alternative_options": getattr(self, "alternative_options", None),
            "report_author": getattr(self, "report_author", None),
            "interests_declared": getattr(self, "interests_declared", None),
            "call_in_deadline": getattr(self, "call_in_deadline", None),
            "call_in_count": getattr(self, "call_in_count", None),
            "wards_affected": getattr(self, "wards_affected", None),
            "install_url": getattr(self, "install_url", None),
        }
        RAW_FIELDS = ["identifier", "title", "decision_maker", "date"]
        for attr in RAW_FIELDS:
            out["raw_{}".format(attr)] = getattr(self, attr)

        return out

    def as_json(self):
        return json.dumps(self.as_dict(), indent=4, sort_keys=True)
