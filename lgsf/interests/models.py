import json
from dataclasses import dataclass, field
from pathlib import Path

from slugify import slugify


@dataclass
class RegisterOfInterestsBase:
    url: str
    identifier: str
    councillor_name: str
    councillor_id: str
    councillor_url: str = None
    division: str = None
    interests: list = field(init=False, hash=False, compare=False)
    documents: list = field(init=False, hash=False, compare=False)
    published_date: str = field(init=False, hash=False, compare=False)

    def __repr__(self):
        return f"<RegisterOfInterests: {self.councillor_name} ({self.identifier})>"

    def __hash__(self):
        return hash(self.identifier)

    def __eq__(self, other):
        return (
            issubclass(type(other), RegisterOfInterestsBase)
            and self.identifier == other.identifier
        )

    def as_file_name(self):
        return f"{slugify(self.councillor_id)}-{slugify(self.councillor_name)}"

    @classmethod
    def from_storage(cls, filename: Path, session):
        """Load a register of interests from storage."""
        data = json.loads(session.open(filename))

        interests = data.pop("interests", [])
        documents = data.pop("documents", [])
        published_date = data.pop("published_date", None)
        division = data.pop("division", None)

        for k in list(data.keys()):
            if k.startswith("raw_"):
                data[k[4:]] = data.pop(k)

        record = cls(**data)
        record.interests = interests
        record.documents = documents
        record.published_date = published_date
        if division:
            record.division = division
        return record

    def as_dict(self):
        out = {
            "url": self.url,
            "councillor_url": getattr(self, "councillor_url", None),
            "division": getattr(self, "division", None),
            "published_date": getattr(self, "published_date", None),
            "interests": getattr(self, "interests", []),
            "documents": getattr(self, "documents", []),
        }
        RAW_FIELDS = ["identifier", "councillor_name", "councillor_id"]
        for attr in RAW_FIELDS:
            out[f"raw_{attr}"] = getattr(self, attr)

        return out

    def as_json(self):
        return json.dumps(self.as_dict(), indent=4, sort_keys=True)
