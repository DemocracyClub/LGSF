import datetime
import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Dict

from rich.table import Table


class RunStatus(Enum):
    OK = 0
    ERROR = 1


@dataclass
class RunLog:
    """Class for keeping track of a single run of a scraper."""

    start: datetime.datetime = 0
    end: datetime.datetime = 0
    duration: datetime.timedelta = 0
    log: str = ""
    error: str = ""
    status_code: int = RunStatus.OK.value

    def finish(self):
        self.end = datetime.datetime.utcnow()
        self.duration = self.end - self.start
        if self.error:
            self.status_code = RunStatus.ERROR.value

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
