import csv
import json
from dataclasses import dataclass, field

from slugify import slugify

@dataclass
class CouncillorBase:
    url: str
    identifier: str
    name: str
    party: str
    division: str
    email: str = field(init=False, hash=False, compare=False)
    photo_url: str = field(init=False, hash=False, compare=False)

    def __repr__(self):
        return "<Councillor: Name: {}>".format(self.name)

    def __hash__(self):
        return hash(self.identifier)

    def __eq__(self, other):
        return (
            issubclass(type(other), CouncillorBase)
            and self.identifier == other.identifier
        )

    def as_file_name(self):
        return "{}-{}".format(slugify(self.identifier), slugify(self.name))

    def as_csv(self):
        out = csv.StringIO()
        out_csv = csv.writer(out)
        out_csv.writerow([self.identifier, self.name, self.party, self.division])
        return out.getvalue()

    def as_dict(self):
        out = {
            "photo_url": getattr(self, "photo_url", None),
            "email": getattr(self, "email", None),
            "standing_down": getattr(self, "standing_down", None),
            "url": self.url,
        }
        RAW_FIELDS = ["identifier", "name", "party", "division"]
        for attr in RAW_FIELDS:
            out["raw_{}".format(attr)] = getattr(self, attr)

        return out

    def as_json(self):
        return json.dumps(self.as_dict(), indent=4)
