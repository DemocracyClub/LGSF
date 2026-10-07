"""
Live progress for long local runs over many councils.

A scraper reports what it is doing to ``self.progress``. By default that is a
NullProgress, which does nothing: Lambda runs one council per invocation
with nobody watching, and a nightly run is short, so there is nothing to
show and no state to share. Only a local run over several councils at once,
where each council's own output is held back until it finishes, swaps in a
LiveProgress, so that a worker that has stalled can be told from one that
is just busy.
"""

import threading
import time

from rich.console import Group
from rich.table import Table
from rich.text import Text


class NullProgress:
    """Accepts progress reports and does nothing with them."""

    def set(self, **fields):
        pass

    def add(self, field, amount=1):
        pass

    def doing(self, activity, limit=None):
        pass


class CouncilProgress(NullProgress):
    """One council's progress, as shown in one row of the live table."""

    def __init__(self, council):
        self.council = council
        self.fields = {}
        self.activity = "starting"
        self.activity_since = time.monotonic()
        self.activity_limit = None
        self.finished = False
        self.failed = False

    def set(self, **fields):
        self.fields.update(fields)

    def add(self, field, amount=1):
        self.fields[field] = self.fields.get(field, 0) + amount

    def doing(self, activity, limit=None):
        """
        Say what the scraper is doing now. ``limit`` is how many seconds it
        should take at most, beyond which the row shows it as overdue.
        """
        self.activity = activity
        self.activity_since = time.monotonic()
        self.activity_limit = limit

    def get(self, field, default=0):
        return self.fields.get(field, default)


def format_bytes(count):
    for unit in ("B", "KB", "MB", "GB"):
        if count < 1024:
            return f"{count:.0f} {unit}" if unit == "B" else f"{count:.1f} {unit}"
        count /= 1024
    return f"{count:.1f} TB"


class LiveProgress:
    """
    Every council in a local run, rendered as a table for rich.live.Live.

    Councils that have finished drop out of the table into the totals, so
    the table is only ever as long as the number of workers.
    """

    def __init__(self, total):
        self.total = total
        self.started = time.monotonic()
        self._councils = {}
        self._lock = threading.Lock()

    def for_council(self, council):
        progress = CouncilProgress(council)
        with self._lock:
            self._councils[council] = progress
        return progress

    def finish(self, council, failed=False):
        with self._lock:
            progress = self._councils.get(council)
        if progress:
            progress.finished = True
            progress.failed = failed or progress.get("given_up", False)

    def __rich__(self):
        with self._lock:
            councils = list(self._councils.values())
        running = [c for c in councils if not c.finished]
        finished = [c for c in councils if c.finished]
        failed = [c.council for c in finished if c.failed]

        downloaded = sum(c.get("documents_downloaded") for c in councils)
        size = sum(c.get("bytes") for c in councils)
        elapsed = int(time.monotonic() - self.started)
        summary = Text.assemble(
            (f"{len(finished)}/{self.total} councils done", "bold"),
            f", {len(running)} running · ",
            f"{sum(c.get('decisions_done') for c in councils)} decisions · ",
            f"{downloaded} documents ({format_bytes(size)}) downloaded · ",
            f"{elapsed // 3600}h{elapsed // 60 % 60:02d}m",
        )
        if failed:
            summary.append(f"\nGave up or failed: {' '.join(sorted(failed))}", "red")

        table = Table(expand=True)
        table.add_column("Council", style="magenta", no_wrap=True)
        table.add_column("Slice", no_wrap=True)
        table.add_column("Decisions", justify="right", no_wrap=True)
        table.add_column("Docs new/kept/failed", justify="right", no_wrap=True)
        table.add_column("Data", justify="right", no_wrap=True)
        table.add_column("Requests", justify="right", no_wrap=True)
        table.add_column("Retries", justify="right", no_wrap=True)
        table.add_column("Now", overflow="ellipsis", no_wrap=True, ratio=1)

        now = time.monotonic()
        for c in sorted(running, key=lambda c: c.council):
            table.add_row(
                c.council,
                self.slice_text(c),
                self.decisions_text(c),
                f"{c.get('documents_downloaded')}/{c.get('documents_skipped')}"
                f"/{c.get('documents_failed')}",
                format_bytes(c.get("bytes")),
                str(c.get("requests")),
                self.retries_text(c),
                self.activity_text(c, now),
            )
        return Group(summary, table)

    def slice_text(self, c):
        window = c.get("window", None)
        if not window:
            return ""
        return f"{window} ({c.get('windows_done')}/{c.get('windows_total')})"

    def decisions_text(self, c):
        text = f"{c.get('window_decisions_done')}/{c.get('window_decisions')}"
        failed = c.get("decisions_failed")
        if failed:
            return Text.assemble(text, (f" ({failed} failed)", "yellow"))
        return text

    def retries_text(self, c):
        retries = c.get("retries")
        return Text(str(retries), style="yellow" if retries else "")

    def activity_text(self, c, now):
        seconds = int(now - c.activity_since)
        style = ""
        if c.activity_limit is not None and seconds > c.activity_limit:
            # Longer than it can legitimately take: the thing to look at.
            style = "bold red"
        elif seconds > 60:
            style = "yellow"
        return Text(f"{seconds:>4}s {c.activity}", style=style)
