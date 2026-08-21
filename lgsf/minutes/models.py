import json
from dataclasses import dataclass, field
from pathlib import Path

from slugify import slugify


@dataclass
class MeetingBase:
    url: str
    identifier: str
    title: str
    committee: str
    date: str
    committee_id: str = None
    status: str = field(init=False, hash=False, compare=False)
    location: str = field(init=False, hash=False, compare=False)
    minutes_published: bool = field(init=False, hash=False, compare=False)
    documents: list = field(init=False, hash=False, compare=False)
    # Where this meeting's detail was fetched from, plus any HTTP validators
    # the server gave us, so the next run can ask "has this changed?"
    source: dict = field(init=False, hash=False, compare=False)

    def __repr__(self):
        return "<Meeting: {} on {}>".format(self.committee, self.date)

    def __hash__(self):
        return hash(self.identifier)

    def __eq__(self, other):
        return (
            issubclass(type(other), MeetingBase) and self.identifier == other.identifier
        )

    def as_file_name(self):
        return (
            f"{slugify(self.date)}-{slugify(self.identifier)}-{slugify(self.committee)}"
        )

    @classmethod
    def from_storage(cls, filename: Path, session):
        """Load meeting from storage session"""
        data = json.loads(session.open(filename))

        status = data.pop("status", None)
        location = data.pop("location", None)
        minutes_published = data.pop("minutes_published", None)
        documents = data.pop("documents", None)
        source = data.pop("source", None)
        for k in list(data.keys()):
            if k.startswith("raw_"):
                data[k[4:]] = data.pop(k)

        meeting = cls(**data)
        if status:
            meeting.status = status
        if location:
            meeting.location = location
        if minutes_published is not None:
            meeting.minutes_published = minutes_published
        if documents is not None:
            meeting.documents = documents
        if source is not None:
            meeting.source = source
        return meeting

    def as_dict(self):
        out = {
            "url": self.url,
            "status": getattr(self, "status", None),
            "location": getattr(self, "location", None),
            "minutes_published": getattr(self, "minutes_published", None),
            "documents": getattr(self, "documents", []),
            "source": getattr(self, "source", {}),
        }
        RAW_FIELDS = ["identifier", "title", "committee", "committee_id", "date"]
        for attr in RAW_FIELDS:
            out["raw_{}".format(attr)] = getattr(self, attr)

        return out

    def as_json(self):
        return json.dumps(self.as_dict(), indent=4, sort_keys=True)
