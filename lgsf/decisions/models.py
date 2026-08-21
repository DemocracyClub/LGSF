import json
from dataclasses import dataclass, field
from pathlib import Path

from slugify import slugify


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
        }
        RAW_FIELDS = ["identifier", "title", "decision_maker", "date"]
        for attr in RAW_FIELDS:
            out["raw_{}".format(attr)] = getattr(self, attr)

        return out

    def as_json(self):
        return json.dumps(self.as_dict(), indent=4, sort_keys=True)
