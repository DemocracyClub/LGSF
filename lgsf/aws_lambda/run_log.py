import json
from dataclasses import dataclass, asdict
import datetime
from typing import Dict

from rich.table import Table


@dataclass
class RunLog:
    """Class for keeping track of a single run of a scraper."""

    start: datetime.datetime = 0
    end: datetime.datetime = 0
    duration: datetime.timedelta = 0
    log: str = ""
    error: str = ""
    status_codes: dict = None

    def finish(self):
        self.end = datetime.datetime.utcnow()
        self.duration = self.end - self.start

    @property
    def as_dict(self) -> Dict:
        return asdict(self)

    @property
    def as_json(self) -> str:
        return json.dumps(self.as_dict, default=str)

    @property
    def as_rich_table(self):
        table = Table(title="Run report")
        table.add_column("Key", style="magenta")
        table.add_column("Value", style="green")
        for key, value in self.as_dict.items():
            table.add_row(key, str(value))

        return table
