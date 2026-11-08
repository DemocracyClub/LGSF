"""Naming files, and choosing a council's or a year's."""

import json

import pytest

from lgsf.embeddings.files import File, council_files, files_for_year, parse_file
from lgsf.embeddings.stores import LocalStore

A = "a" * 64
B = "b" * 64
C = "c" * 64


def test_file_names():
    file = File("STO", f"{A}.pdf")
    assert file.digest == A
    assert file.content_hash == f"sha256:{A}"
    assert file.document_key == f"STO/documents/{A}.pdf"
    assert File("STO", A).digest == A


def test_parse_file():
    assert parse_file(f"sto/{A}.pdf") == File("STO", f"{A}.pdf")
    assert parse_file(f"/STO/{A}.pdf/") == File("STO", f"{A}.pdf")


@pytest.mark.parametrize(
    "value",
    [
        "STO",
        f"{A}.pdf",
        f"STO/documents/{A}.pdf",
        "STO/2026-07-15-2547-s81425.pdf",
        f"STO/{A}.pdf.status.json",
    ],
)
def test_parse_file_refuses_other_names(value):
    with pytest.raises(ValueError):
        parse_file(value)


def test_council_files(tmp_path):
    store = LocalStore(tmp_path)
    for name in (f"{B}.pdf", f"{A}.html", f"{A}.html.status.json", f"{B}.pdf.md", C):
        store.write_bytes(f"STO/documents/{name}", b"")
    store.write_bytes("STO/documents/2026-07-15-2547-s81425.pdf", b"")
    store.write_bytes(f"OLD/documents/{A}.pdf", b"")
    assert council_files(store, "STO") == [
        File("STO", f"{A}.html"),
        File("STO", f"{B}.pdf"),
        File("STO", C),
    ]
    assert council_files(store, "NEW") == []


def record(documents):
    return json.dumps({"documents": documents}).encode()


def test_files_for_year(tmp_path):
    store = LocalStore(tmp_path)
    store.write_bytes(
        "STO/Minutes/json/2026-10-20-1664-development-control-committee.json",
        record([{"storage_key": f"{B}.pdf"}, {"title": "Not downloaded"}]),
    )
    store.write_bytes(
        "STO/Minutes/json/2026-01-01-1-cabinet.json",
        record([{"storage_key": f"{B}.pdf"}, {"storage_key": f"{A}.pdf"}]),
    )
    store.write_bytes(
        "STO/Decisions/json/2026-03-01-12.json",
        record([{"storage_key": f"{C}.docx"}]),
    )
    store.write_bytes(
        "STO/Decisions/json/2025-12-31-11.json", record([{"storage_key": A}])
    )
    store.write_bytes("STO/Minutes/json/2026-not-a-record.txt", b"x")
    store.write_bytes("STO/Minutes/json/2026-02-02-2-bad.json", b"{")
    assert files_for_year(store, "STO", 2026) == [
        File("STO", f"{A}.pdf"),
        File("STO", f"{B}.pdf"),
        File("STO", f"{C}.docx"),
    ]
    assert files_for_year(store, "STO", "2025") == [File("STO", A)]
    assert files_for_year(store, "STO", 2024) == []


def test_files_for_year_wants_a_year(tmp_path):
    with pytest.raises(ValueError):
        files_for_year(LocalStore(tmp_path), "STO", "26")
