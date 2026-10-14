"""
The SQL search() builds, its filters, and the snippets shown in results.

No database: a fake connection records the query and returns set rows.
Tests that build SQL need psycopg, from the embeddings dependency group,
and are skipped without it.
"""

import datetime

import pytest

from lgsf.embeddings.models import ChunkType
from lgsf.embeddings.search import (
    SearchFilters,
    SearchResult,
    search,
    snippet,
    table_name,
)
from lgsf.embeddings.tests.test_vectors import FakeEmbedder


class FakeConnection:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query.as_string(), params))
        return self

    def fetchall(self):
        return self.rows


def test_no_filters():
    assert SearchFilters().where() == ("TRUE", {})


def test_filters():
    filters = SearchFilters(
        councils=["EDE", "ADU"],
        types=[ChunkType.DECISIONS],
        since=datetime.date(2026, 1, 1),
        until=datetime.date(2026, 3, 31),
    )
    where, params = filters.where()
    assert where == (
        "c.council = ANY(%(councils)s) AND c.type = ANY(%(types)s) "
        "AND c.date >= %(since)s AND c.date <= %(until)s"
    )
    assert params == {
        "councils": ["EDE", "ADU"],
        "types": ["decisions"],
        "since": datetime.date(2026, 1, 1),
        "until": datetime.date(2026, 3, 31),
    }


def test_table_name_matches_load_py():
    assert table_name("titan-v2") == "embedding_titan_v2"
    assert table_name("bge-m3") == "embedding_bge_m3"


def test_snippet_puts_text_on_one_line():
    assert snippet("RESOLVED:\n\n1. That Cabinet  notes it.", 100) == (
        "RESOLVED: 1. That Cabinet notes it."
    )


def test_snippet_cuts_at_a_word():
    assert snippet("That Cabinet notes the report.", 16) == "That Cabinet …"


def test_search_sends_query_vector_filters_and_limit():
    pytest.importorskip("psycopg")
    conn = FakeConnection()
    embedder = FakeEmbedder()
    search(conn, embedder, "council tax", SearchFilters(councils=["EDE"]), limit=5)
    [(query, params)] = conn.executed
    assert '"embedding_fake"' in query
    assert "c.council = ANY(%(councils)s)" in query
    assert params == {
        "councils": ["EDE"],
        "query": str(embedder.embed_query("council tax")),
        "limit": 5,
    }


def test_search_returns_one_chunk_per_source_by_default():
    pytest.importorskip("psycopg")
    conn = FakeConnection()
    search(conn, FakeEmbedder(), "council tax")
    search(conn, FakeEmbedder(), "council tax", one_per_source=False)
    assert "DISTINCT ON (c.parent_id)" in conn.executed[0][0]
    assert "DISTINCT ON" not in conn.executed[1][0]


def test_search_results():
    pytest.importorskip("psycopg")
    row = (
        "EDE:minutes:2599:17076:1",
        "East Devon District Council · Council · 25 February 2026",
        "RESOLVED that the council tax be set.",
        {"meeting_url": "https://democracy.eastdevon.gov.uk/"},
        0.461,
    )
    [result] = search(FakeConnection([row]), FakeEmbedder(), "council tax")
    assert result == SearchResult(
        chunk_id="EDE:minutes:2599:17076:1",
        distance=0.461,
        context="East Devon District Council · Council · 25 February 2026",
        text="RESOLVED that the council tax be set.",
        metadata={"meeting_url": "https://democracy.eastdevon.gov.uk/"},
    )
