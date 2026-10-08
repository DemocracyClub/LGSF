from rich.progress import BarColumn, Progress, TimeElapsedColumn
from rich.table import Table

from lgsf.commands.base import Council, CouncilFilteringCommandBase
from lgsf.conf import settings
from lgsf.embeddings.chunkers import ChunkerBase, ModGovMinutesChunker


class Command(CouncilFilteringCommandBase):
    """
    Chunk councils' scraped minutes, ready for creating embeddings.

    Reads the data already in data/<COUNCIL>/ and writes
    data/<COUNCIL>/Embeddings/<type>.jsonl. Nothing is fetched.

    Usage:
        python manage.py embeddings --council EDE
        python manage.py embeddings --all-councils
    """

    command_name = "embeddings"

    #: Chunker for each minutes CMS, from services.minutes.cms_type in the
    #: council's metadata.json.
    CHUNKERS = {
        "ModernGov": ModGovMinutesChunker,
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--council",
            action="store",
            help="The council code(s) to chunk (comma-separated)",
        )
        parser.add_argument(
            "--all-councils",
            action="store_true",
            help="Chunk all current councils",
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

    def handle(self, options):
        self.options = options
        if not (options["council"] or options["all_councils"]):
            self.parser.error("one of --council or --all-councils required")

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
                task = progress.add_task("Chunking", total=len(councils))
                for council in councils:
                    results.append(self.chunk_council(council))
                    progress.update(task, advance=1)
        else:
            for council in councils:
                results.append(self.chunk_council(council))

        self.output_summary(results)
        return 0

    def output_summary(self, results):
        results = sorted(results, key=lambda r: r["council"])
        chunked = [r for r in results if "skipped" not in r]
        skipped = [r for r in results if "skipped" in r]

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

        if skipped:
            table = Table(title="Skipped")
            table.add_column("Council", style="magenta")
            table.add_column("Reason", style="yellow")
            for result in skipped:
                table.add_row(result["council"], result["skipped"])
            self.console.print(table)
