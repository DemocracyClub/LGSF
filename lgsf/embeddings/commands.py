import time

from rich.progress import BarColumn, Progress, TimeElapsedColumn
from rich.table import Table

from lgsf.commands.base import Council, CouncilFilteringCommandBase
from lgsf.conf import settings
from lgsf.embeddings.chunkers import ChunkerBase, ModGovMinutesChunker
from lgsf.embeddings.embedders import EMBEDDERS, EmbedderBase
from lgsf.embeddings.vectors import (
    embed_chunks,
    embeddings_path,
    read_chunks,
    read_embeddings,
    write_embeddings,
)
from lgsf.path_utils import data_abs_path


class Command(CouncilFilteringCommandBase):
    """
    Chunk councils' scraped minutes, and create embeddings of the chunks.

    `chunk` reads the data already in data/<COUNCIL>/ and writes
    data/<COUNCIL>/Embeddings/<type>.jsonl. Nothing is fetched.

    `embed` reads those chunk files and writes
    data/<COUNCIL>/Embeddings/<model>/<type>.jsonl. A chunk embedded
    before, by the same model and with the same text, keeps its embedding
    rather than being sent to the model again. It needs the embeddings
    dependency group: `uv sync --group embeddings`.

    Usage:
        python manage.py embeddings chunk --council EDE
        python manage.py embeddings chunk --all-councils
        AWS_PROFILE=dev-lgsf-dc python manage.py embeddings embed --model titan-v2 --council EDE
        python manage.py embeddings embed --model bge-m3 --all-councils
    """

    command_name = "embeddings"

    #: Chunker for each minutes CMS, from services.minutes.cms_type in the
    #: council's metadata.json.
    CHUNKERS = {
        "ModernGov": ModGovMinutesChunker,
    }

    def add_arguments(self, parser):
        # So that the subcommands' usage reads "manage.py embeddings chunk".
        parser.prog = f"{parser.prog} {self.command_name}"
        subparsers = parser.add_subparsers(dest="subcommand", required=True)

        chunk = subparsers.add_parser("chunk", help="Chunk councils' scraped minutes")
        embed = subparsers.add_parser(
            "embed", help="Create embeddings of councils' chunks with one model"
        )
        embed.add_argument(
            "--model",
            required=True,
            choices=sorted(EMBEDDERS),
            help="The embedding model to use",
        )
        for subparser, verb in ((chunk, "chunk"), (embed, "embed")):
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
        if options["subcommand"] == "chunk":
            return self.chunk()
        return self.embed()

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
            table = Table(title="Skipped")
            table.add_column("Council", style="magenta")
            table.add_column("Reason", style="yellow")
            for result in skipped:
                table.add_row(result["council"], result["skipped"])
            self.console.print(table)

    # manage.py embeddings chunk

    def chunk(self):
        results = self.run_councils("Chunking", self.chunk_council)
        self.output_chunk_summary(results)
        return 0

    def chunk_council(self, council: Council) -> dict:
        """Chunk and write one council's data, returning what to report."""
        result = {"council": council.council_id}
        try:
            cms_type = council.service_metadata("minutes").get("cms_type")
        except FileNotFoundError:
            result["skipped"] = "no such council"
            return result

        chunker_cls = self.CHUNKERS.get(cms_type)
        if not chunker_cls:
            result["skipped"] = f"no chunker for {cms_type or 'unknown'} minutes"
            return result

        try:
            chunker = chunker_cls(council.council_id)
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
                "items": len({c.parent_id for c in chunks}),
                "chunks": len(chunks),
                "split_items": len({c.parent_id for c in chunks if c.chunk_count > 1}),
                "not_embedded": sum(1 for c in chunks if not c.embed),
            }
        )
        return result

    def output_chunk_summary(self, results):
        results = sorted(results, key=lambda r: r["council"])
        chunked = [r for r in results if "skipped" not in r]

        if chunked:
            table = Table(title="Chunked minutes")
            table.add_column("Council", style="magenta")
            table.add_column("Meetings read", justify="right")
            table.add_column("Items", justify="right")
            table.add_column("Chunks", justify="right")
            table.add_column("Items split", justify="right")
            table.add_column("Not embedded", justify="right")
            for result in chunked:
                table.add_row(
                    result["council"],
                    str(result["sources_read"]),
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

    def embed(self):
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
