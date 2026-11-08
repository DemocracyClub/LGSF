"""
``manage.py embeddings``: run the document pipeline on one file, one
council, or one year of a council's files. See docs/document-pipeline.md.

    manage.py embeddings --council STO --dry-run
    manage.py embeddings --council STO --year 2026
    manage.py embeddings --file STO/<sha256>.pdf --embedder titan-v2
    manage.py embeddings --council STO --source s3 --stages extract,chunk

Extracting and chunking need the embeddings dependency group:
``uv sync --group embeddings``. Embedding with titan-v2 needs AWS
credentials with bedrock:InvokeModel; the default embedder is the fake
one, which needs nothing.
"""

import argparse
import collections
import contextlib
import importlib.util
import time
from concurrent.futures.process import BrokenProcessPool

from lgsf.commands.base import CommandBase
from lgsf.embeddings import pipeline
from lgsf.embeddings.embedders import EMBEDDERS
from lgsf.embeddings.extract import (
    ConverterSettings,
    Device,
    ExtractStatus,
    worker_pool,
)
from lgsf.embeddings.files import council_files, files_for_year, parse_file
from lgsf.embeddings.sidecars import Stage
from lgsf.embeddings.stores import Source, store_for


def parse_stages(value):
    try:
        return [Stage(stage.strip()) for stage in value.split(",")]
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid stages {value!r}, expected a comma-separated list "
            f"of {', '.join(Stage)}"
        ) from None


def parse_file_argument(value):
    try:
        return parse_file(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


class Command(CommandBase):
    command_name = "embeddings"

    def add_arguments(self, parser):
        parser.description = (
            "Extract, chunk and embed stored documents, writing the results "
            "beside each document"
        )
        parser.add_argument(
            "--council",
            help="The council code. Required unless --file is given.",
        )
        parser.add_argument(
            "--file",
            type=parse_file_argument,
            help="One file, as COUNCIL/<storage_key>",
        )
        parser.add_argument(
            "--year",
            type=int,
            help="Only the files linked from the council's meetings and "
            "decisions dated in this year (YYYY)",
        )
        parser.add_argument(
            "--stages",
            type=parse_stages,
            default=list(Stage),
            help=f"Which stages to run (default {','.join(Stage)})",
        )
        parser.add_argument(
            "--embedder",
            choices=sorted(EMBEDDERS),
            default="fake",
            help="The embedding model (default fake, which calls nothing)",
        )
        parser.add_argument(
            "--source",
            choices=list(Source),
            type=Source,
            default=Source.LOCAL,
            help="Where the documents are: the data directory (local, the "
            "default) or the bucket LGSF_S3_BUCKET and LGSF_S3_PREFIX name (s3)",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Run every stage again, even for files already done",
        )
        parser.add_argument(
            "--retry-failed",
            action="store_true",
            help="Extract again files that failed or timed out before",
        )
        parser.add_argument(
            "--device",
            choices=list(Device),
            type=Device,
            default=Device.AUTO,
            help="What Docling runs its models on (default auto)",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=2,
            help="How many files to convert at once, each in its own "
            "process with its own copy of Docling's models (default 2)",
        )
        parser.add_argument(
            "--threads",
            type=int,
            default=4,
            help="How many threads each worker uses (default 4)",
        )
        parser.add_argument(
            "--timeout",
            type=float,
            default=1800,
            help="Give up on a file after this many seconds (default 1800)",
        )
        parser.add_argument(
            "--max-pages",
            type=int,
            help="Leave out PDFs with more pages than this, for a later run",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Say what would be done, without doing it",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Only the first this many files",
        )

    def write(self, text=""):
        self.console.print(text, markup=False, highlight=False, soft_wrap=True)

    def handle(self, options):
        self.options = options
        if options["file"] is None and not options["council"]:
            self.parser.error("one of --council or --file is required")
        if options["file"] is not None and options["year"] is not None:
            self.parser.error(
                "--year chooses a council's files; it can't go with --file"
            )

        store = store_for(options["source"])
        files = self.get_files(store)
        embedders = []
        if Stage.EMBED in options["stages"]:
            embedders = [EMBEDDERS[options["embedder"]]()]
        pipeline_options = pipeline.Options(
            stages=options["stages"],
            max_pages=options["max_pages"],
            force=options["force"],
            retry_failed=options["retry_failed"],
            converter=ConverterSettings(
                threads=options["threads"],
                timeout=options["timeout"],
                device=options["device"],
            ),
        )

        names = [embedder.name for embedder in embedders] or [options["embedder"]]
        if len(files) == 1:
            plans = [pipeline.plan(store, files[0], pipeline_options, names)]
        else:
            plans = pipeline.plan_files(store, files, pipeline_options, names)
        self.output_plan(files, plans, store)
        if options["dry_run"]:
            return
        self.run(store, files, plans, embedders, pipeline_options)

    def get_files(self, store):
        options = self.options
        if options["file"] is not None:
            files = [options["file"]]
        else:
            council = options["council"].upper()
            if options["year"] is not None:
                files = files_for_year(store, council, options["year"])
            else:
                files = council_files(store, council)
        if options["limit"] is not None:
            files = files[: options["limit"]]
        return files

    def output_plan(self, files, plans, store):
        """What a run would do, from the files' statuses."""
        self.write(f"{len(files)} files in {store!r}")
        outcomes = collections.Counter()
        needs = collections.Counter()
        for file_plan in plans:
            status = file_plan.status
            outcomes[
                str(status.outcome) if status and status.outcome else "no status"
            ] += 1
            if file_plan.extract:
                needs["extract"] += 1
            elif file_plan.chunk:
                needs["chunk"] += 1
            elif file_plan.embed:
                needs["embed"] += 1
            else:
                needs["nothing"] += 1
        self.output_counts("Outcome so far", outcomes)
        self.output_counts(
            "To do (the first stage each file needs)",
            needs,
            order=["extract", "chunk", "embed", "nothing"],
        )

    def run(self, store, files, plans, embedders, pipeline_options):
        options = self.options
        to_convert = [p for p in plans if p.needs_converter]
        if to_convert and importlib.util.find_spec("docling") is None:
            self.parser.error(
                "docling isn't installed. Run: uv sync --group embeddings"
            )

        self.outcomes = []
        self.to_do = sum(not p.nothing_to_do for p in plans)
        self.write()
        start = time.monotonic()
        executor = (
            worker_pool(options["workers"], pipeline_options.converter)
            if to_convert
            else contextlib.nullcontext()
        )
        stopped = None
        try:
            with executor:
                pipeline.run_files(
                    store,
                    files,
                    pipeline_options,
                    embedders,
                    executor if to_convert else None,
                    self.output_outcome,
                    plans=plans,
                )
        except KeyboardInterrupt:
            stopped = "Stopped."
        except BrokenProcessPool:
            # Raised once by run_files(), after every file still in the
            # pool has been reported.
            stopped = (
                "A worker process stopped, perhaps because it ran out of "
                "memory. Fewer --workers use less."
            )
        self.output_summary(time.monotonic() - start)
        if stopped:
            self.parser.error(
                f"{stopped} What's been written is kept: run the same "
                "command again to carry on."
            )

    def output_outcome(self, outcome):
        """A line for each file that needed something, as it's finished."""
        if outcome.skipped:
            return
        self.outcomes.append(outcome)
        result = outcome.result
        parts = []
        if outcome.error:
            parts.append(f"error: {outcome.error.strip().splitlines()[-1]}")
        elif result.missing:
            parts.append("not found")
        elif result.too_long:
            parts.append(f"left out, {result.too_long} pages")
        else:
            status = outcome.status
            if result.extract:
                extract = result.extract
                parts.append(
                    " ".join(
                        str(part)
                        for part in (
                            status.format,
                            extract.status,
                            f"{extract.pages} pages" if extract.pages else "",
                            f"{extract.seconds}s",
                        )
                        if part
                    )
                )
                if status.error:
                    parts.append(f"error: {status.error}")
            if result.chunk:
                if result.chunk.error:
                    parts.append(f"chunk error: {result.chunk.error}")
                else:
                    parts.append(f"{result.chunk.chunks} chunks")
            for name in outcome.embedded_with:
                entry = status.embed[name]
                parts.append(f"{name} {entry.embedded} embedded, {entry.reused} reused")
                if entry.error:
                    parts.append(f"{name} error: {entry.error}")
            if outcome.note:
                parts.append(outcome.note)
        self.write(
            f"[{len(self.outcomes)}/{self.to_do}] {outcome.file.council} "
            f"{outcome.file.storage_key}: " + "; ".join(parts)
        )

    def output_summary(self, elapsed):
        extracted = collections.Counter()
        totals = collections.Counter()
        for outcome in self.outcomes:
            result = outcome.result
            if outcome.error:
                totals["errors"] += 1
                continue
            if result.missing:
                totals["not found"] += 1
            if result.too_long:
                totals["too long"] += 1
            if result.extract:
                extracted[str(result.extract.status)] += 1
                totals["pages"] += result.extract.pages or 0
            if result.chunk:
                if result.chunk.error:
                    totals["chunk errors"] += 1
                else:
                    totals["chunked"] += 1
                    totals["chunks"] += result.chunk.chunks
            if outcome.status:
                for name in outcome.embedded_with:
                    entry = outcome.status.embed[name]
                    if entry.error:
                        totals["embed errors"] += 1
                    totals["embedded"] += entry.embedded
                    totals["tokens"] += entry.tokens

        self.output_counts("Extracted", extracted, order=list(ExtractStatus))
        self.output_counts(
            "Totals",
            totals,
            order=[
                "pages",
                "chunked",
                "chunks",
                "embedded",
                "tokens",
                "not found",
                "too long",
                "chunk errors",
                "embed errors",
                "errors",
            ],
        )
        rate = f", {elapsed / totals['pages']:.2f}s a page" if totals["pages"] else ""
        self.write(f"\n{len(self.outcomes)} files worked on in {elapsed:.0f}s{rate}")

    def output_counts(self, title, counts, order=None):
        if not any(counts.values()):
            return
        self.write(f"\n{title}:")
        names = order or [name for name, _ in counts.most_common()]
        for name in names:
            if counts[name]:
                self.write(f"  {counts[name]:>7}  {name}")
