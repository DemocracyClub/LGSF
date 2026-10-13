# /// script
# requires-python = ">=3.12"
# dependencies = ["psycopg[binary]", "pgvector", "numpy"]
# ///
"""
Load LGSF chunks and their embeddings into Postgres with pgvector.

Reads the files written by `manage.py embeddings chunk` and
`manage.py embeddings embed`:

    <data-dir>/<COUNCIL>/Embeddings/<type>.jsonl           chunks
    <data-dir>/<COUNCIL>/Embeddings/<model>/<type>.jsonl   embeddings

into a `chunk` table and one `embedding_<model>` table per model, with
`-` in the model's name replaced by `_`.

Each run drops, recreates and loads `chunk` and the tables of the models
it's given, in one transaction. Tables of other models are left alone.

This doesn't import from lgsf, so it can be moved to another project. The
JSONL files are the interface: if lgsf's Chunk gains a field, add it here
to load it.

Usage:
    docker compose -f lgsf/embeddings/db/compose.yaml up -d
    uv run lgsf/embeddings/db/load.py --data-dir data --model titan-v2 --model bge-m3

Connects to $DATABASE_URL, or the database in compose.yaml if it's unset.
"""

import argparse
import itertools
import json
import os
import re
import sys
from pathlib import Path

import numpy as np
import psycopg
from pgvector.psycopg import register_vector
from psycopg import sql
from psycopg.types.json import Jsonb

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/embeddings"

#: Model names become table names, so only these characters are allowed.
MODEL_NAME = re.compile(r"^[a-z0-9-]+$")

CREATE_CHUNK = """
CREATE TABLE chunk (
    chunk_id     text PRIMARY KEY,
    parent_id    text NOT NULL,
    council      text NOT NULL,
    type         text NOT NULL,
    source_type  text NOT NULL,
    date         date,
    context      text NOT NULL,
    text         text NOT NULL,
    chunk_index  integer NOT NULL,
    chunk_count  integer NOT NULL,
    embed        boolean NOT NULL,
    metadata     jsonb NOT NULL,
    text_hash    text NOT NULL
)
"""

CHUNK_COLUMNS = [
    "chunk_id",
    "parent_id",
    "council",
    "type",
    "source_type",
    "date",
    "context",
    "text",
    "chunk_index",
    "chunk_count",
    "embed",
    "metadata",
    "text_hash",
]

# No foreign key to chunk, so chunk can be reloaded on its own. Embeddings
# of old text are found by comparing text_hash instead; see check().
# No vector index: at around 11,000 rows an exact scan is fast enough,
# and its results are exact.
CREATE_EMBEDDING = """
CREATE TABLE {table} (
    chunk_id     text PRIMARY KEY,
    model        text NOT NULL,
    text_hash    text NOT NULL,
    token_count  integer NOT NULL,
    embedding    vector({dimensions}) NOT NULL
)
"""

CHECK_EMBEDDINGS = """
SELECT
    count(*) FILTER (WHERE c.embed AND e.chunk_id IS NULL),
    count(*) FILTER (WHERE e.text_hash <> c.text_hash),
    count(*) FILTER (WHERE c.chunk_id IS NULL)
FROM chunk c
FULL JOIN {table} e USING (chunk_id)
"""


class LoadError(Exception):
    pass


def read_lines(paths):
    for path in paths:
        with path.open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)


def table_name(model_name):
    return f"embedding_{model_name.replace('-', '_')}"


def chunk_files(data_dir):
    return sorted(data_dir.glob("*/Embeddings/*.jsonl"))


def embedding_files(data_dir, model_name):
    return sorted(data_dir.glob(f"*/Embeddings/{model_name}/*.jsonl"))


def load_chunks(cur, paths):
    cur.execute("DROP TABLE IF EXISTS chunk")
    cur.execute(CREATE_CHUNK)
    copy_sql = sql.SQL("COPY chunk ({}) FROM STDIN").format(
        sql.SQL(", ").join(map(sql.Identifier, CHUNK_COLUMNS))
    )
    count = 0
    with cur.copy(copy_sql) as copy:
        for chunk in read_lines(paths):
            row = [chunk[column] for column in CHUNK_COLUMNS]
            row[CHUNK_COLUMNS.index("metadata")] = Jsonb(chunk["metadata"])
            copy.write_row(row)
            count += 1
    return count


def load_embeddings(cur, model_name, paths):
    """
    Load one model's embeddings, returning (rows, model ID, dimensions).

    The vector size comes from the first record. Every record must have
    the same model ID and size; if one doesn't, nothing is loaded.
    """
    records = read_lines(paths)
    first = next(records, None)
    if first is None:
        raise LoadError(f"{model_name}'s files are empty")
    model, dimensions = first["model"], len(first["embedding"])

    table = sql.Identifier(table_name(model_name))
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
    cur.execute(
        sql.SQL(CREATE_EMBEDDING).format(
            table=table, dimensions=sql.Literal(dimensions)
        )
    )
    copy_sql = sql.SQL(
        "COPY {} (chunk_id, model, text_hash, token_count, embedding) "
        "FROM STDIN WITH (FORMAT BINARY)"
    ).format(table)
    count = 0
    with cur.copy(copy_sql) as copy:
        copy.set_types(["text", "text", "text", "int4", "vector"])
        for record in itertools.chain([first], records):
            if record["model"] != model:
                raise LoadError(
                    f"{record['chunk_id']} is from {record['model']}; "
                    f"the other {model_name} records are from {model}"
                )
            if len(record["embedding"]) != dimensions:
                raise LoadError(
                    f"{record['chunk_id']} has {len(record['embedding'])} "
                    f"dimensions; the other {model_name} records have "
                    f"{dimensions}"
                )
            copy.write_row(
                [
                    record["chunk_id"],
                    record["model"],
                    record["text_hash"],
                    record["token_count"],
                    np.asarray(record["embedding"], dtype=np.float32),
                ]
            )
            count += 1
    return count, model, dimensions


def check(cur, model_name):
    """
    (missing, stale, orphaned) for one model's table:

    - missing: chunks with embed=True that have no embedding
    - stale: embeddings of text the chunk no longer has, because the
      chunks were remade after they were embedded
    - orphaned: embeddings of chunks that no longer exist
    """
    table = sql.Identifier(table_name(model_name))
    cur.execute(sql.SQL(CHECK_EMBEDDINGS).format(table=table))
    return cur.fetchone()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--data-dir",
        required=True,
        type=Path,
        help="LGSF's data directory, containing a directory per council",
    )
    parser.add_argument(
        "--model",
        action="append",
        default=[],
        help="Name of a model whose embeddings to load, e.g. titan-v2. "
        "Can be given more than once.",
    )
    args = parser.parse_args()

    # Checked before connecting, so a typo doesn't drop any tables.
    chunk_paths = chunk_files(args.data_dir)
    if not chunk_paths:
        parser.error(f"no chunk files in {args.data_dir}/*/Embeddings/")
    embedding_paths = {}
    for model_name in args.model:
        if not MODEL_NAME.match(model_name):
            parser.error(f"model names can only contain a-z, 0-9 and -: {model_name}")
        embedding_paths[model_name] = embedding_files(args.data_dir, model_name)
        if not embedding_paths[model_name]:
            parser.error(f"no files in {args.data_dir}/*/Embeddings/{model_name}/")

    database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    report = []
    try:
        with psycopg.connect(database_url) as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            # Before opening the cursor: a cursor copies the connection's
            # types when it's created, and COPY needs the vector type.
            register_vector(conn)

            with conn.cursor() as cur:
                report.append(f"chunk: {load_chunks(cur, chunk_paths)} rows")
                for model_name, paths in embedding_paths.items():
                    count, model, dimensions = load_embeddings(cur, model_name, paths)
                    missing, stale, orphaned = check(cur, model_name)
                    report.append(
                        f"{table_name(model_name)}: {count} rows "
                        f"({model}, {dimensions} dimensions); "
                        f"{missing} missing, {stale} stale, {orphaned} orphaned"
                    )
    except LoadError as error:
        sys.exit(f"Nothing loaded: {error}")
    # Only once the transaction has committed.
    print("\n".join(report))


if __name__ == "__main__":
    main()
