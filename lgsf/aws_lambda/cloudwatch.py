# cloudwatch_rich_stream.py
import sys
import time
import threading
from io import IOBase
from typing import List, Dict, Optional
import boto3
from botocore.exceptions import ClientError

class CloudWatchLogStream(IOBase):
    """
    File-like stream that batches writes and sends to CloudWatch Logs.
    Use one instance per Lambda invocation (unique stream name).
    """
    def __init__(
        self,
        log_group: str,
        log_stream: str,
        flush_interval: float = 0.2,      # seconds
        max_batch_bytes: int = 900_000,   # keep under 1MB per PutLogEvents call
        tee_stdout: bool = True
    ):
        self.logs = boto3.client("logs")
        self.log_group = log_group
        self.log_stream = log_stream
        self.flush_interval = flush_interval
        self.max_batch_bytes = max_batch_bytes
        self.tee_stdout = tee_stdout

        self._buf = ""                  # partial line buffer
        self._events: List[Dict] = []   # accumulated events (dicts)
        self._events_bytes = 0
        self._seq: Optional[str] = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._flusher = threading.Thread(target=self._run_flusher, daemon=True)

        self._ensure_group_and_stream()
        self._flusher.start()

    # ---- standard file-like API ----
    def write(self, s: str) -> int:
        if not s:
            return 0
        now_ms = int(time.time() * 1000)
        out = s

        # tee to stdout so you still get native Lambda logs
        if self.tee_stdout:
            try:
                sys.__stdout__.write(out)
                sys.__stdout__.flush()
            except Exception:
                pass

        with self._lock:
            self._buf += out
            # split into complete lines
            while True:
                if "\n" not in self._buf:
                    break
                line, self._buf = self._buf.split("\n", 1)
                msg = line.rstrip("\r")
                if not msg:
                    continue
                # enforce ~256KB/event cap
                b = msg.encode("utf-8")
                if len(b) > 240_000:
                    msg = b[:240_000].decode("utf-8", "ignore") + " …(truncated)"
                    b = msg.encode("utf-8")
                self._events.append({"timestamp": now_ms, "message": msg})
                self._events_bytes += len(b) + 26  # small overhead estimate
                if self._events_bytes >= self.max_batch_bytes:
                    self._flush_locked()

        return len(s)

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def close(self) -> None:
        self._stop.set()
        self._flusher.join(timeout=1.0)
        # flush any remaining partial line as an event
        with self._lock:
            if self._buf:
                self._events.append({
                    "timestamp": int(time.time() * 1000),
                    "message": self._buf
                })
                self._buf = ""
            self._flush_locked()

    # ---- internals ----
    def _run_flusher(self):
        # periodic flush to respect PutLogEvents rate limits
        while not self._stop.wait(self.flush_interval):
            try:
                self.flush()
            except Exception:
                # never let flusher crash the process
                pass

    def _ensure_group_and_stream(self):
        try:
            self.logs.create_log_group(logGroupName=self.log_group)
        except self.logs.exceptions.ResourceAlreadyExistsException:
            pass
        try:
            self.logs.create_log_stream(
                logGroupName=self.log_group,
                logStreamName=self.log_stream
            )
            self._seq = None
        except self.logs.exceptions.ResourceAlreadyExistsException:
            # fetch current sequence token
            resp = self.logs.describe_log_streams(
                logGroupName=self.log_group,
                logStreamNamePrefix=self.log_stream,
                limit=1
            )
            streams = [s for s in resp.get("logStreams", [])
                       if s["logStreamName"] == self.log_stream]
            if streams:
                self._seq = streams[0].get("uploadSequenceToken")

    def _flush_locked(self):
        if not self._events:
            return
        # CloudWatch requires non-decreasing timestamps
        self._events.sort(key=lambda e: e["timestamp"])
        kwargs = dict(
            logGroupName=self.log_group,
            logStreamName=self.log_stream,
            logEvents=self._events
        )
        if self._seq:
            kwargs["sequenceToken"] = self._seq
        try:
            resp = self.logs.put_log_events(**kwargs)
            self._seq = resp["nextSequenceToken"]
            self._events.clear()
            self._events_bytes = 0
        except self.logs.exceptions.InvalidSequenceTokenException as e:
            # recover by extracting expected token and retry once
            msg = str(e)
            token = None
            # rudimentary parse
            if "expected sequenceToken is" in msg:
                token = msg.rsplit(" ", 1)[-1]
            if token:
                self._seq = token
                resp = self.logs.put_log_events(**{**kwargs, "sequenceToken": self._seq})
                self._seq = resp["nextSequenceToken"]
                self._events.clear()
                self._events_bytes = 0
            else:
                raise
        except ClientError as e:
            # swallow throttles; CW Logs is eventually consistent; try again next tick
            if e.response["Error"]["Code"] in ("ThrottlingException", "ServiceUnavailableException"):
                return
            raise
