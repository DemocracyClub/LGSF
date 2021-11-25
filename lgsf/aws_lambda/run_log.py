import json
from dataclasses import dataclass, asdict
import datetime


@dataclass
class RunLog:
    """Class for keeping track of a single run of a scraper."""

    start: datetime.datetime = 0
    end: datetime.datetime = 0
    duration: datetime.timedelta = 0
    log: str = ""
    error: str = ""
    status_codes: dict = None

    @property
    def as_json(self) -> str:
        return json.dumps(asdict(self), default=str)
