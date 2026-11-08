"""
Naming the files the pipeline works on, and choosing them.

A file is (council, storage_key), where the storage key is the one
lgsf.storage.documents gives a document: its SHA-256 as hex and the
extension its bytes were sniffed as. It's stored at
``<COUNCIL>/documents/<storage_key>`` in the data directory and the
bucket alike.
"""

import json
import re
from dataclasses import dataclass

from lgsf.storage.documents.base import is_content_key
from lgsf.storage.s3 import DOCUMENTS_DIR

#: The data types whose records link documents. Only used to choose
#: files by the year of the records that link them.
RECORD_DIRS = ("Minutes", "Decisions")

_YEAR = re.compile(r"^\d{4}$")


@dataclass(frozen=True, order=True)
class File:
    council: str
    storage_key: str

    @property
    def digest(self) -> str:
        """The file's SHA-256 as hex: the storage key without its extension."""
        return self.storage_key.split(".", 1)[0]

    @property
    def content_hash(self) -> str:
        return f"sha256:{self.digest}"

    @property
    def document_key(self) -> str:
        """Where the file is, relative to the data directory."""
        return f"{self.council}/{DOCUMENTS_DIR}/{self.storage_key}"


def parse_file(value: str) -> File:
    """
    A File from ``CODE/<storage_key>``, as ``--file`` is given. The
    council code is upper-cased.
    """
    council, separator, storage_key = value.strip().strip("/").partition("/")
    if not separator or not council or not storage_key or "/" in storage_key:
        raise ValueError(f"Expected COUNCIL/<storage_key>, not {value!r}")
    if not is_content_key(storage_key):
        raise ValueError(
            f"{storage_key!r} isn't a document's storage key: those are the "
            "file's SHA-256 as 64 hex digits and an extension"
        )
    return File(council.upper(), storage_key)


def council_files(store, council: str) -> list[File]:
    """
    Every document of a council in the store, in key order. The files
    written beside documents aren't documents, and are left out by
    their names.
    """
    prefix = f"{council}/{DOCUMENTS_DIR}/"
    return [
        File(council, key.removeprefix(prefix))
        for key in store.list_prefix(prefix)
        if is_content_key(key.removeprefix(prefix))
    ]


def files_for_year(store, council: str, year: int | str) -> list[File]:
    """
    The documents linked from a council's records dated in ``year``: the
    meetings and decisions whose file names start with that year, read
    for the storage keys of their documents. In key order, each once.
    """
    year = str(year)
    if not _YEAR.match(year):
        raise ValueError(f"Expected a year as YYYY, not {year!r}")
    keys = set()
    for record_dir in RECORD_DIRS:
        for record_key in store.list_prefix(f"{council}/{record_dir}/json/{year}-"):
            if not record_key.endswith(".json"):
                continue
            try:
                record = json.loads(store.read_bytes(record_key))
            except (ValueError, FileNotFoundError):
                continue
            for document in record.get("documents") or []:
                storage_key = (document or {}).get("storage_key")
                if storage_key and is_content_key(storage_key):
                    keys.add(storage_key)
    return [File(council, key) for key in sorted(keys)]
