import csv
import json

from slugify import slugify


class CouncillorBase:
    def __init__(
        self, url, identifier=None, name=None, party=None, division=None
    ):
        self.url = url.strip()
        self.identifier = identifier.strip()
        self.name = name.strip()
        self.party = party.strip()
        self.division = division.strip()

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
        out_csv.writerow(
            [self.identifier, self.name, self.party, self.division]
        )
        return out.getvalue()

    def as_dict(self):
        out = {"email": getattr(self, "email", None), "url": self.url}
        RAW_FIELDS = ["identifier", "name", "party", "division"]
        for attr in RAW_FIELDS:
            out["raw_{}".format(attr)] = getattr(self, attr)

        return out

    def as_json(self):
        return json.dumps(self.as_dict(), indent=4)
