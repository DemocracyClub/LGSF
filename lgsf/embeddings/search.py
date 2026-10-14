"""
Searching the embeddings that lgsf/embeddings/db/load.py loaded into
Postgres.

A question is embedded with the same model as the table it's searched
against, and the chunks are ranked by cosine distance from it: 0 is the
same direction, larger is further away. There's no vector index, so every
row is compared.

psycopg is imported inside connect(), so this module imports without the
`embeddings` dependency group installed.
"""

import datetime
from dataclasses import dataclass, field

from lgsf.embeddings.embedders import EmbedderBase

#: The database in lgsf/embeddings/db/compose.yaml. Set DATABASE_URL
#: to use another.
DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/embeddings"

# {distinct} and {order} pick between every matching chunk and only the
# closest chunk of each source, so a long agenda item split into several
# chunks doesn't fill the results.
SEARCH_SQL = """
SELECT chunk_id, context, text, metadata, distance
FROM (
    SELECT {distinct} c.chunk_id, c.parent_id, c.context, c.text, c.metadata,
           e.embedding <=> %(query)s::vector AS distance
    FROM {table} e
    JOIN chunk c USING (chunk_id)
    WHERE {where}
    ORDER BY {order}
) matches
ORDER BY distance
LIMIT %(limit)s
"""


@dataclass
class SearchFilters:
    #: Council codes; empty for all councils.
    councils: list[str] = field(default_factory=list)
    #: Chunk types, such as "minutes"; empty for all types.
    types: list[str] = field(default_factory=list)
    #: Only chunks dated on or after this.
    since: datetime.date | None = None
    #: Only chunks dated on or before this.
    until: datetime.date | None = None

    def where(self) -> tuple[str, dict]:
        """The WHERE condition, and the parameters it uses."""
        conditions = []
        params = {}
        if self.councils:
            conditions.append("c.council = ANY(%(councils)s)")
            params["councils"] = list(self.councils)
        if self.types:
            conditions.append("c.type = ANY(%(types)s)")
            params["types"] = [str(t) for t in self.types]
        if self.since:
            conditions.append("c.date >= %(since)s")
            params["since"] = self.since
        if self.until:
            conditions.append("c.date <= %(until)s")
            params["until"] = self.until
        return " AND ".join(conditions) or "TRUE", params


@dataclass
class SearchResult:
    chunk_id: str
    distance: float
    context: str
    text: str
    metadata: dict


def table_name(model_name: str) -> str:
    """The table load.py puts a model's embeddings in, e.g. embedding_titan_v2."""
    return f"embedding_{model_name.replace('-', '_')}"


def snippet(text: str, length: int) -> str:
    """The start of `text` on one line, cut at a word if it's too long."""
    flat = " ".join(text.split())
    if len(flat) <= length:
        return flat
    return flat[:length].rsplit(" ", 1)[0] + " …"


def connect(database_url: str = DEFAULT_DATABASE_URL):
    import psycopg

    return psycopg.connect(database_url)


def has_table(conn, model_name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s)", [table_name(model_name)]).fetchone()
    return row[0] is not None


def search(
    conn,
    embedder: EmbedderBase,
    question: str,
    filters: SearchFilters = None,
    limit: int = 10,
    one_per_source: bool = True,
) -> list[SearchResult]:
    """
    The chunks closest to `question`, closest first.

    With one_per_source, only the closest chunk of each source (each
    agenda item, for minutes, each decision and each document) is
    returned.
    """
    from psycopg import sql

    where, params = (filters or SearchFilters()).where()
    if one_per_source:
        distinct = sql.SQL("DISTINCT ON (c.parent_id)")
        order = sql.SQL("c.parent_id, distance")
    else:
        distinct = sql.SQL("")
        order = sql.SQL("distance")
    query = sql.SQL(SEARCH_SQL).format(
        distinct=distinct,
        table=sql.Identifier(table_name(embedder.name)),
        where=sql.SQL(where),
        order=order,
    )
    # Sent as pgvector's text format, "[0.1, -0.2, ...]", and cast in the
    # SQL, so pgvector's Python package isn't needed.
    params.update({"query": str(embedder.embed_query(question)), "limit": limit})
    rows = conn.execute(query, params).fetchall()
    return [
        SearchResult(
            chunk_id=chunk_id,
            distance=distance,
            context=context,
            text=text,
            metadata=metadata,
        )
        for chunk_id, context, text, metadata, distance in rows
    ]
