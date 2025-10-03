from __future__ import annotations

import json
import re
from importlib import import_module
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Tuple

from lgsf.conf import settings


def _abs_path(base_dir, code, mkdir=False) -> Tuple[Path, str]:
    """
    Find the path to a directory based on the code.

    Directories can be scrapers or data, both are in the same format.

    This isn't as simple as it could be due to the directory being in the format
    [org code]-[org slug]. This means we need to glob for the directory and
    return the first match.

    We have support for legacy [code] only directories as a fallback.

    Lastly we scan for any directory with the code in any case. This is to deal
    with odd cases, e.g the GLA, where we don't have a three letter code.

    Raises:
        FileNotFoundError: If no matching scraper file is found.
    """
    abs_path = Path(base_dir).expanduser().resolve()
    code = code.upper()

    # 1) Match CODE-* in the same dir
    for file_path in abs_path.glob(f"{code}-*"):
        parts = file_path.name.split("-")
        if parts[0] == code:
            return file_path, parts[0]  # returns uppercased token

    # 2) Exact match on a path named CODE
    abs_path_root = abs_path / code
    if abs_path_root.exists():
        return abs_path_root, code

    # 3) Fallback: scan everything in base_dir
    for file_path in abs_path.iterdir():
        file_name = file_path.name

        # Equivalent to re.match("{}-[a-z\-]+", file_name) at start of string
        if re.match(rf"^{re.escape(code)}-[a-z\-]+", file_name):
            return file_path, code  # keep original casing for this branch

        parts = file_name.lower().split("-")
        if code.lower() in parts:
            return file_path, parts[0]  # returns lowercased first token

    if mkdir:
        new_dir = abs_path / code
        new_dir.mkdir(exist_ok=True)
        return new_dir, code
    raise FileNotFoundError(f"No file for code {code} at path {abs_path}")


def scraper_abs_path(code=None) -> Path:
    return _abs_path(settings.SCRAPER_DIR_NAME, code)[0]


def data_abs_path(code: str, mkdir=False):
    return _abs_path(settings.DATA_DIR_NAME, code, mkdir=mkdir)[0]


def create_org_package(name) -> Path:
    path = settings.BASE_PATH / settings.SCRAPER_DIR_NAME / name
    path.mkdir(exist_ok=True)
    return path


def load_scraper(code, command):
    from lgsf.scrapers import ScraperBase

    path = scraper_abs_path(code) / f"{command}.py"
    if not path.exists():
        return False
    scraper_module = SourceFileLoader("module.name", str(path)).load_module()
    scraper_class = scraper_module.Scraper
    if not issubclass(scraper_class, ScraperBase):
        raise ValueError(
            f"Scraper at {path} must be a subclass of lgsf.scrapers.BaseScraper"
        )
    return scraper_class


def load_council_info(code):
    path = scraper_abs_path(code) / "metadata.json"
    if path.exists():
        with path.open() as f:
            return json.loads(f.read())
    return None


def load_command_module(module_name):
    return import_module(f"{module_name}.{settings.COMMAND_FILE_NAME}")


def load_command(module_name):
    module = load_command_module(module_name)
    if hasattr(module, "Command"):
        return module.Command
    raise ValueError(f"{module_name} doesn't contain a class")
