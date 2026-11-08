"""
split_document(), the table serializer it uses and first_heading(),
against a real Manchester decision notice converted with Docling
2.130.0: two pages of tables of labels and values, with no header
rows, in four sections.

These tests need docling-core, from the embeddings dependency group,
and are skipped without it.
"""

from pathlib import Path

import pytest

pytest.importorskip("docling_core")

import pandas as pd  # noqa: E402
from docling_core.types.doc import DoclingDocument  # noqa: E402

from lgsf.embeddings.documents import (  # noqa: E402
    first_heading,
    split_document,
    table_lines,
)

FIXTURE = Path(__file__).parent / "fixtures" / "MAN" / "decision-notice.docling.json"


@pytest.fixture(scope="module")
def document():
    return DoclingDocument.load_from_json(FIXTURE)


@pytest.fixture(scope="module")
def parts(document):
    return split_document(document)


def test_parts(parts):
    assert len(parts) == 4
    assert parts[0].text == "Made by an Executive Member or Chief Officer"


def test_headings_and_pages(parts):
    assert parts[1].headings == ["Notice of Decision Made"]
    assert parts[1].pages == [1, 2]


def test_tables_with_no_header_row_are_labels_and_values(parts):
    lines = parts[1].text.split("\n")
    assert lines[:2] == [
        "Decision maker: City Treasurer and Interim Deputy Chief Executive",
        "Date of decision: 6 August 2026",
    ]
    assert ", 1 =" not in "\n".join(part.text for part in parts)


def test_max_words(document):
    parts = split_document(document, max_words=20)
    assert len(parts) > 4
    assert all(len(part.text.split()) <= 20 for part in parts)


def test_first_heading(document):
    assert first_heading(document) == "Decision taken"


def test_table_with_no_header_row():
    table = pd.DataFrame(
        [
            ["Decision maker", "City Treasurer"],
            ["Spans both columns", "Spans both columns"],
            ["Not filled in", ""],
            ["Two values", "One"],
        ]
    )
    table[2] = ["", "", "", "Two"]
    assert table_lines(table) == [
        "Decision maker: City Treasurer",
        "Spans both columns",
        "Not filled in",
        "Two values: One | Two",
    ]


def test_table_with_a_header_row():
    table = pd.DataFrame(
        [
            ["Axewoods Co-Op", "£1,395", "Agreed"],
            ["Seaton Hall", "", "Deferred"],
        ],
        columns=["Applicant", "Amount", "Decision"],
    )
    assert table_lines(table) == [
        "Axewoods Co-Op, Amount = £1,395. Axewoods Co-Op, Decision = Agreed",
        "Seaton Hall, Decision = Deferred",
    ]


def test_table_with_only_a_header_row():
    table = pd.DataFrame([], columns=["Applicant", "Amount"])
    assert table_lines(table) == ["Applicant | Amount"]
