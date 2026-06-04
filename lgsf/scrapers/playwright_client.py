import os
import tempfile

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


class PlaywrightHTTPClient:
    """Headless Chromium HTTP client for JS-challenge-protected sites (e.g. Incapsula).

    In Lambda, uses launch_persistent_context() with an explicit /tmp user data dir
    because the rest of the Lambda filesystem is read-only. Outside Lambda, uses a
    standard launch() with an ephemeral context.
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
        self._pw = sync_playwright().start()
        self._browser = None
        in_lambda = bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))

        if in_lambda:
            # Lambda's filesystem is read-only except /tmp. Chromium needs to write its
            # user data dir somewhere, so we create an explicit path under /tmp.
            user_data_dir = tempfile.mkdtemp(dir="/tmp")
            self._context = self._pw.chromium.launch_persistent_context(
                user_data_dir,
                headless=headless,
                args=self._LAMBDA_ARGS,
                user_agent=self._USER_AGENT,
            )
        else:
            self._browser = self._pw.chromium.launch(headless=headless)
            self._context = self._browser.new_context(user_agent=self._USER_AGENT)

        self.timeout = timeout

    def get(self, url, headers=None, timeout=None):
        page = self._context.new_page()
        try:
            if headers:
                page.set_extra_http_headers(headers)
            response = page.goto(
                url,
                timeout=(timeout or self.timeout) * 1000,
                wait_until="networkidle",
            )
            status = response.status if response else 200
            # For XML responses (e.g. ModGov ASMX endpoints), Chrome wraps the content
            # in its XML viewer UI. response.body() gives the raw response bytes instead.
            # For HTML responses, page.content() gives the JS-rendered DOM (needed for
            # Incapsula/Cloudflare-protected HTML pages like STH).
            content_type = response.headers.get("content-type", "") if response else ""
            if "xml" in content_type and response:
                content = response.body().decode("utf-8", errors="replace")
            else:
                content = page.content()
            return PlaywrightResponse(status, content)
        finally:
            page.close()

    def close(self):
        try:
            self._context.close()
            if self._browser:
                self._browser.close()
            self._pw.stop()
        except Exception:
            pass

    def __del__(self):
        self.close()
