import json
from dataclasses import dataclass, field
from pathlib import Path

from slugify import slugify


#: Fields written only when the source gives them, read back as they are.
EXTRA_FIELDS = (
    "times",
    "published",
    "is_webcast",
    "agenda_items",
    "attendance",
    "declarations",
    "install_url",
)


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

    # What the source says about the meeting beyond the basics, where it
    # says it (ModernGov does; other systems fill what they can). See
    # docs/minutes-scrapers.md for each field.
    #: Times as the source gives them, e.g. {"scheduled": "7.00 pm",
    #: "actual_start": "7.00 pm", "actual_finish": "2.22 am"}: no dates, and
    #: an actual finish can be after midnight.
    times: dict = field(init=False, hash=False, compare=False)
    #: {"agenda": bool, "decisions": bool, "minutes": bool}
    published: dict = field(init=False, hash=False, compare=False)
    is_webcast: bool = field(init=False, hash=False, compare=False)
    #: In the source's order, each with the ids of its documents
    agenda_items: list = field(init=False, hash=False, compare=False)
    #: One entry per person the source lists, with how they attended
    attendance: list = field(init=False, hash=False, compare=False)
    #: Declarations of interest, where the source itemises them (CMIS
    #: does; ModernGov gives only a count per agenda item)
    declarations: list = field(init=False, hash=False, compare=False)
    #: The base URL of the system the meeting was read from. Councils that
    #: share one ModernGov install list the same meetings, and joint
    #: committees appear on several councils' sites: the same install and
    #: identifier is the same meeting.
    install_url: str = field(init=False, hash=False, compare=False)

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
        extras = {name: data.pop(name, None) for name in EXTRA_FIELDS}
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
        for name, value in extras.items():
            if value is not None:
                setattr(meeting, name, value)
        return meeting

    def as_dict(self):
        out = {
            "url": self.url,
            "status": getattr(self, "status", None),
            "location": getattr(self, "location", None),
            "minutes_published": getattr(self, "minutes_published", None),
            "documents": getattr(self, "documents", []),
            "source": getattr(self, "source", {}),
            "times": getattr(self, "times", {}),
            "published": getattr(self, "published", {}),
            "is_webcast": getattr(self, "is_webcast", None),
            "agenda_items": getattr(self, "agenda_items", []),
            "attendance": getattr(self, "attendance", []),
            "declarations": getattr(self, "declarations", []),
            "install_url": getattr(self, "install_url", None),
        }
        RAW_FIELDS = ["identifier", "title", "committee", "committee_id", "date"]
        for attr in RAW_FIELDS:
            out["raw_{}".format(attr)] = getattr(self, attr)

        return out

    def as_json(self):
        return json.dumps(self.as_dict(), indent=4, sort_keys=True)
