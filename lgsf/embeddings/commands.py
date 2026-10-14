import argparse
import datetime
import os
import sys
import time

from rich.markup import escape
from rich.padding import Padding
from rich.progress import BarColumn, Progress, TimeElapsedColumn
from rich.table import Table

from lgsf.commands.base import Council, CouncilFilteringCommandBase
from lgsf.conf import settings
from lgsf.embeddings.chunkers import (
    ChunkerBase,
    DecisionChunker,
    ModGovMinutesChunker,
)
from lgsf.embeddings.embedders import EMBEDDERS, EmbedderBase
from lgsf.embeddings.models import ChunkType
from lgsf.embeddings.search import (
    DEFAULT_DATABASE_URL,
    SearchFilters,
    connect,
    has_table,
    search,
    snippet,
    table_name,
)
from lgsf.embeddings.vectors import (
    embed_chunks,
    embeddings_path,
    read_chunks,
    read_embeddings,
    write_embeddings,
)
from lgsf.path_utils import data_abs_path


def chunk_types(value: str) -> list[ChunkType]:
    """The value of `chunk --type`: chunk types, comma-separated."""
    types = []
    for name in value.split(","):
        name = name.strip()
        if not name:
            continue
        try:
            types.append(ChunkType(name))
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"{name!r} isn't a chunk type. Choose from: {', '.join(ChunkType)}"
            )
    return types


class Command(CouncilFilteringCommandBase):
    """
    Chunk councils' scraped data, create embeddings of the chunks, and
    search them.

    `chunk` reads the data already in data/<COUNCIL>/ and writes
    data/<COUNCIL>/Embeddings/<type>.jsonl, one file for each type of
    data. Each run of `chunk` replaces the files of the types it's run
    for, and leaves the others alone. Nothing is fetched.

    `embed` reads those chunk files and writes
    data/<COUNCIL>/Embeddings/<model>/<type>.jsonl. A chunk embedded
    before, by the same model and with the same text, keeps its embedding
    rather than being sent to the model again.

    `search` searches the embeddings loaded by lgsf/embeddings/db/load.py
    with a question. It shows the closest agenda items first, one chunk
    from each, with the start of its text and a link to the meeting. With
    no question, it asks for one question after another until it's given
    a blank line. bge-m3 takes several seconds to load, so this loads it
    once for all of them. It needs the database from
    lgsf/embeddings/db/compose.yaml or DATABASE_URL.

    `embed` and `search` need the embeddings dependency group:
    `uv sync --group embeddings`.

    Usage:
        python manage.py embeddings chunk --council EDE
        python manage.py embeddings chunk --all-councils
        python manage.py embeddings chunk --type decisions --since 2026-01-01 --council MAN,PEM
        AWS_PROFILE=dev-lgsf-dc python manage.py embeddings embed --model titan-v2 --council EDE
        python manage.py embeddings embed --model bge-m3 --all-councils
        AWS_PROFILE=dev-lgsf-dc python manage.py embeddings search --model titan-v2 "how much is council tax going up"
        python manage.py embeddings search --model bge-m3 --council EDE --since 2026-01-01
    """

    command_name = "embeddings"

    #: Chunker for each minutes CMS, from services.minutes.cms_type in the
    #: council's metadata.json.
    MINUTES_CHUNKERS = {
        "ModernGov": ModGovMinutesChunker,
    }

    #: Chunker for each other type of data. These don't depend on the CMS:
    #: every decisions scraper writes the same JSON.
    CHUNKERS = {
        ChunkType.DECISIONS: DecisionChunker,
    }

    #: How much of each search result's text is shown, in characters,
    #: unless --full is given.
    snippet_length = 300

    def add_arguments(self, parser):
        # So that the subcommands' usage reads "manage.py embeddings chunk".
        parser.prog = f"{parser.prog} {self.command_name}"
        subparsers = parser.add_subparsers(dest="subcommand", required=True)

        chunk_parser = subparsers.add_parser(
            "chunk", help="Chunk councils' scraped data"
        )
        chunk_parser.add_argument(
            "--type",
            type=chunk_types,
            default=list(ChunkType),
            help="The types of data to chunk, comma-separated, from: "
            f"{', '.join(ChunkType)}. Default: all of them.",
        )
        chunk_parser.add_argument(
            "--since",
            type=datetime.date.fromisoformat,
            help="Only chunk what's dated on or after this date (YYYY-MM-DD)",
        )
        chunk_parser.add_argument(
            "--until",
            type=datetime.date.fromisoformat,
            help="Only chunk what's dated on or before this date (YYYY-MM-DD)",
        )
        embed_parser = subparsers.add_parser(
            "embed", help="Create embeddings of councils' chunks with one model"
        )
        embed_parser.add_argument(
            "--model",
            required=True,
            choices=sorted(EMBEDDERS),
            help="The embedding model to use",
        )
        for subparser, verb in ((chunk_parser, "chunk"), (embed_parser, "embed")):
            councils = subparser.add_mutually_exclusive_group(required=True)
            councils.add_argument(
                "--council",
                action="store",
                help=f"The council code(s) to {verb} (comma-separated)",
            )
            councils.add_argument(
                "--all-councils",
                action="store_true",
                help=f"{verb.capitalize()} all current councils",
            )

        search_parser = subparsers.add_parser(
            "search", help="Search the embeddings in Postgres with a question"
        )
        search_parser.add_argument(
            "question",
            nargs="?",
            help="What to search for. Leave out to be asked for questions "
            "one after another.",
        )
        search_parser.add_argument(
            "--model",
            required=True,
            choices=sorted(EMBEDDERS),
            help="The model whose embeddings to search",
        )
        search_parser.add_argument(
            "--council",
            help="Only these council codes (comma-separated)",
        )
        search_parser.add_argument(
            "--since",
            type=datetime.date.fromisoformat,
            help="Only meetings on or after this date (YYYY-MM-DD)",
        )
        search_parser.add_argument(
            "--until",
            type=datetime.date.fromisoformat,
            help="Only meetings on or before this date (YYYY-MM-DD)",
        )
        search_parser.add_argument(
            "--limit",
            type=int,
            default=10,
            help="How many results to show (default 10)",
        )
        search_parser.add_argument(
            "--every-chunk",
            action="store_true",
            help="Show every matching chunk, not just the closest one from "
            "each agenda item",
        )
        search_parser.add_argument(
            "--full",
            action="store_true",
            help="Show each result's whole text",
        )

    @property
    def councils_to_run(self) -> list[Council]:
        if self.options["all_councils"]:
            return self.current_councils
        return [
            Council(code.strip().split("-")[0].upper())
            for code in self.options["council"].split(",")
            if code.strip()
        ]

    def handle(self, options):
        self.options = options
        handlers = {
            "chunk": self.handle_chunk,
            "embed": self.handle_embed,
            "search": self.handle_search,
        }
        return handlers[options["subcommand"]]()

    def run_councils(self, description, run_council):
        """
        Call run_council() for each council, with a progress bar if there's
        more than one, and return what each call returned.
        """
        councils = self.councils_to_run
        results = []
        if self.pretty and len(councils) > 1:
            with Progress(
                "[progress.description]{task.description}",
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn(),
                console=self.console,
                transient=True,
            ) as progress:
                task = progress.add_task(description, total=len(councils))
                for council in councils:
                    progress.update(
                        task, description=f"{description} {council.council_id}"
                    )
                    results.append(run_council(council))
                    progress.update(task, advance=1)
        else:
            for council in councils:
                results.append(run_council(council))
        return results

    def output_skipped(self, results):
        skipped = [r for r in results if "skipped" in r]
        if skipped:
            with_type = any("type" in r for r in skipped)
            table = Table(title="Skipped")
            table.add_column("Council", style="magenta")
            if with_type:
                table.add_column("Type")
            table.add_column("Reason", style="yellow")
            for result in skipped:
                type_cell = [result.get("type", "")] if with_type else []
                table.add_row(result["council"], *type_cell, result["skipped"])
            self.console.print(table)

    # manage.py embeddings chunk

    def handle_chunk(self):
        results = [
            result
            for council_results in self.run_councils("Chunking", self.chunk_council)
            for result in council_results
        ]
        self.output_chunk_summary(results)
        return 0

    def chunk_council(self, council: Council) -> list[dict]:
        """
        Chunk and write each type of one council's data chosen with --type,
        returning what to report for each.
        """
        try:
            council.metadata
        except FileNotFoundError:
            return [{"council": council.council_id, "skipped": "no such council"}]
        return [
            self.chunk_council_type(council, chunk_type)
            for chunk_type in self.options["type"]
        ]

    def chunker_for(self, council: Council, chunk_type: ChunkType):
        """
        The chunker class for one type of a council's data, or why there
        isn't one.
        """
        if chunk_type == ChunkType.MINUTES:
            cms_type = council.service_metadata("minutes").get("cms_type")
            chunker_cls = self.MINUTES_CHUNKERS.get(cms_type)
            if not chunker_cls:
                return None, f"no chunker for {cms_type or 'unknown'} minutes"
            return chunker_cls, None
        return self.CHUNKERS[chunk_type], None

    def chunk_council_type(self, council: Council, chunk_type: ChunkType) -> dict:
        """Chunk and write one type of one council's data."""
        result = {"council": council.council_id, "type": str(chunk_type)}
        chunker_cls, reason = self.chunker_for(council, chunk_type)
        if not chunker_cls:
            result["skipped"] = reason
            return result

        try:
            chunker = chunker_cls(
                council.council_id,
                since=self.options["since"],
                until=self.options["until"],
            )
        except FileNotFoundError:
            result["skipped"] = "no data"
            return result
        if not chunker.has_data():
            result["skipped"] = f"no {chunker.type} data"
            return result

        chunks = chunker.get_chunks()
        chunker.write(chunks)
        result.update(
            {
                "sources_read": chunker.sources_read,
                "sources_name": chunker.sources_name,
                "items": len({c.parent_id for c in chunks}),
                "chunks": len(chunks),
                "split_items": len({c.parent_id for c in chunks if c.chunk_count > 1}),
                "not_embedded": sum(1 for c in chunks if not c.embed),
            }
        )
        return result

    def output_chunk_summary(self, results):
        results = sorted(results, key=lambda r: (r["council"], r.get("type", "")))
        chunked = [r for r in results if "skipped" not in r]

        if chunked:
            table = Table(title="Chunked")
            table.add_column("Council", style="magenta")
            table.add_column("Type")
            table.add_column("Read", justify="right")
            table.add_column("Items", justify="right")
            table.add_column("Chunks", justify="right")
            table.add_column("Items split", justify="right")
            table.add_column("Not embedded", justify="right")
            for result in chunked:
                table.add_row(
                    result["council"],
                    result["type"],
                    f"{result['sources_read']} {result['sources_name']}",
                    str(result["items"]),
                    str(result["chunks"]),
                    str(result["split_items"]),
                    str(result["not_embedded"]),
                )
            self.console.print(table)
            self.console.print(
                f"[dim]Written to {settings.DATA_DIR_NAME}/<COUNCIL>/"
                f"{ChunkerBase.OUTPUT_DIR_NAME}/[/dim]"
            )

        self.output_skipped(results)

    # manage.py embeddings embed

    def handle_embed(self):
        try:
            embedder = EMBEDDERS[self.options["model"]]()
        except ImportError as error:
            self.parser.error(
                f"{error.name} isn't installed. Run: uv sync --group embeddings"
            )

        start = time.monotonic()
        results = self.run_councils(
            "Embedding", lambda council: self.embed_council(council, embedder)
        )
        self.output_embed_summary(results, embedder, time.monotonic() - start)
        return 0

    def embed_council(self, council: Council, embedder: EmbedderBase) -> dict:
        """Embed and write one council's chunks, returning what to report."""
        result = {"council": council.council_id}
        try:
            chunks_dir = data_abs_path(council.council_id) / ChunkerBase.OUTPUT_DIR_NAME
        except FileNotFoundError:
            result["skipped"] = "no data"
            return result
        chunk_paths = sorted(chunks_dir.glob("*.jsonl"))
        if not chunk_paths:
            result["skipped"] = "not chunked"
            return result

        all_records = []
        embedded = 0
        for chunks_path in chunk_paths:
            path = embeddings_path(chunks_path, embedder.name)
            records, newly_embedded = embed_chunks(
                read_chunks(chunks_path), embedder, read_embeddings(path)
            )
            # A chunk file with nothing to embed gets no embedding file,
            # unless there's one from before to empty.
            if records or path.exists():
                write_embeddings(path, records)
            all_records.extend(records)
            embedded += newly_embedded

        if not all_records:
            result["skipped"] = "no chunks to embed"
            return result
        token_counts = [record.token_count for record in all_records]
        result.update(
            {
                "to_embed": len(all_records),
                "reused": len(all_records) - embedded,
                "embedded": embedded,
                "tokens": sum(token_counts),
                "max_tokens": max(token_counts),
            }
        )
        return result

    def output_embed_summary(self, results, embedder, elapsed):
        results = sorted(results, key=lambda r: r["council"])
        embedded = [r for r in results if "skipped" not in r]

        if embedded:
            table = Table(title=f"Embeddings: {embedder.name} ({embedder.model})")
            table.add_column("Council", style="magenta")
            table.add_column("To embed", justify="right")
            table.add_column("Reused", justify="right")
            table.add_column("Embedded", justify="right")
            table.add_column("Tokens", justify="right")
            table.add_column("Largest", justify="right")
            for result in embedded:
                table.add_row(
                    result["council"],
                    str(result["to_embed"]),
                    str(result["reused"]),
                    str(result["embedded"]),
                    str(result["tokens"]),
                    str(result["max_tokens"]),
                )
            if len(embedded) > 1:
                table.add_section()
                table.add_row(
                    "Total",
                    str(sum(r["to_embed"] for r in embedded)),
                    str(sum(r["reused"] for r in embedded)),
                    str(sum(r["embedded"] for r in embedded)),
                    str(sum(r["tokens"] for r in embedded)),
                    str(max(r["max_tokens"] for r in embedded)),
                )
            self.console.print(table)
            self.console.print(
                f"[dim]Written to {settings.DATA_DIR_NAME}/<COUNCIL>/"
                f"{ChunkerBase.OUTPUT_DIR_NAME}/{embedder.name}/ "
                f"in {elapsed:.0f}s[/dim]"
            )

        self.output_skipped(results)

    # manage.py embeddings search

    def handle_search(self):
        try:
            import psycopg
        except ImportError as error:
            self.parser.error(
                f"{error.name} isn't installed. Run: uv sync --group embeddings"
            )

        model_name = self.options["model"]
        filters = SearchFilters(
            councils=[
                code.strip().upper()
                for code in (self.options["council"] or "").split(",")
                if code.strip()
            ],
            since=self.options["since"],
            until=self.options["until"],
        )

        try:
            conn = connect(os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
        except psycopg.OperationalError as error:
            sys.exit(
                f"Couldn't connect to the database: {error}\n"
                "Start it with: docker compose -f lgsf/embeddings/db/compose.yaml up -d"
            )
        with conn:
            # Checked before loading the model, which can take a while.
            if not has_table(conn, model_name):
                sys.exit(
                    f"There's no {table_name(model_name)} table. Load it with:\n"
                    f"  uv run lgsf/embeddings/db/load.py --data-dir data "
                    f"--model {model_name}"
                )
            embedder = EMBEDDERS[model_name]()

            if self.options["question"]:
                self.output_search_results(
                    conn, embedder, self.options["question"], filters
                )
                return 0

            self.console.print(
                f"[dim]Searching {table_name(model_name)}. A blank line stops.[/dim]"
            )
            while True:
                try:
                    question = self.console.input("[bold]Search:[/bold] ").strip()
                except (EOFError, KeyboardInterrupt):
                    self.console.print()
                    break
                if not question:
                    break
                self.output_search_results(conn, embedder, question, filters)
        return 0

    def output_search_results(self, conn, embedder, question, filters):
        results = search(
            conn,
            embedder,
            question,
            filters=filters,
            limit=self.options["limit"],
            one_per_source=not self.options["every_chunk"],
        )
        if not results:
            self.console.print("[yellow]No results[/yellow]\n")
            return

        for rank, result in enumerate(results, start=1):
            # The context header: "<council> · <committee> · <date>", then
            # "Minute <n>: <title>".
            heading, _, detail = result.context.partition("\n")
            self.console.print(
                f"[bold]{rank:>2}.[/bold] [dim]{result.distance:.3f}[/dim]  "
                f"[bold]{escape(heading)}[/bold]"
            )
            text = (
                result.text
                if self.options["full"]
                else snippet(result.text, self.snippet_length)
            )
            lines = [escape(line) for line in (detail, text) if line]
            url = result.metadata.get("meeting_url")
            if url:
                lines.append(f"[blue]{escape(url)}[/blue]")
            lines.append(f"[dim]{escape(result.chunk_id)}[/dim]")
            self.console.print(Padding("\n".join(lines), (0, 0, 1, 4)))
