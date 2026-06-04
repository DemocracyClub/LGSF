import os
import queue
import tempfile
import threading

from playwright.sync_api import sync_playwright


class PlaywrightResponse:
    def __init__(self, status_code: int, content: str):
        self.status_code = status_code
        self._content = content

    @property
    def text(self):
        return self._content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


_STOP = object()  # sentinel to stop the worker


class PlaywrightHTTPClient:
    """Headless Chromium HTTP client for JS-challenge-protected sites (Incapsula, Cloudflare).

    Runs Playwright in a dedicated background thread so it never conflicts with an
    asyncio event loop in the caller (e.g. Lambda's runtime or CloudWatch log flusher).
    The browser is started once and reused across all get() calls — this is important
    for Incapsula/Cloudflare where the cookie acquired during the JS challenge must
    survive into subsequent page fetches.

    In Lambda, uses launch_persistent_context() with an explicit /tmp user data dir
    because the Lambda filesystem is read-only everywhere except /tmp.
    """

    _LAMBDA_ARGS = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--single-process",
    ]

    _USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    def __init__(self, headless=True, timeout=30):
        self._headless = headless
        self._in_lambda = bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))
        self.timeout = timeout
        self._cmd = queue.Queue()
        self._res = queue.Queue()
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()
        # Block until browser is ready; worker sends True on success or the exception
        result = self._res.get()
        if isinstance(result, Exception):
            raise result

    def _worker(self):
        """Browser lifecycle runs entirely in this thread — no asyncio loop here."""
        try:
            pw = sync_playwright().start()
            if self._in_lambda:
                user_data_dir = tempfile.mkdtemp(dir="/tmp")
                context = pw.chromium.launch_persistent_context(
                    user_data_dir,
                    headless=self._headless,
                    args=self._LAMBDA_ARGS,
                    user_agent=self._USER_AGENT,
                )
                browser = None
            else:
                browser = pw.chromium.launch(headless=self._headless)
                context = browser.new_context(user_agent=self._USER_AGENT)
        except Exception as e:
            self._res.put(e)
            return

        self._res.put(True)  # signal ready

        while True:
            cmd = self._cmd.get()
            if cmd is _STOP:
                break
            url, headers, timeout_ms = cmd
            try:
                page = context.new_page()
                try:
                    if headers:
                        page.set_extra_http_headers(headers)
                    resp = page.goto(url, timeout=timeout_ms, wait_until="networkidle")
                    status = resp.status if resp else 200
                    ct = resp.headers.get("content-type", "") if resp else ""
                    # XML endpoints (ModGov ASMX): Chrome wraps raw XML in its viewer UI,
                    # so use resp.body() to get the real bytes rather than page.content().
                    # HTML endpoints (Incapsula/Cloudflare): page.content() gives the
                    # JS-rendered DOM after the challenge resolves.
                    if "xml" in ct and resp:
                        content = resp.body().decode("utf-8", errors="replace")
                    else:
                        content = page.content()
                    self._res.put((status, content))
                finally:
                    page.close()
            except Exception as e:
                self._res.put(e)

        try:
            context.close()
            if browser:
                browser.close()
            pw.stop()
        except Exception:
            pass

    def get(self, url, headers=None, timeout=None):
        timeout_ms = (timeout or self.timeout) * 1000
        self._cmd.put((url, headers, timeout_ms))
        # Add a buffer beyond playwright's own timeout so it fires first
        result = self._res.get(timeout=(timeout or self.timeout) + 30)
        if isinstance(result, Exception):
            raise result
        status, content = result
        return PlaywrightResponse(status, content)

    def close(self):
        try:
            self._cmd.put(_STOP)
            self._thread.join(timeout=10)
        except Exception:
            pass

    def __del__(self):
        self.close()
