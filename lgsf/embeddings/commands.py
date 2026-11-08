"""
``manage.py embeddings``: run the document pipeline on one file, one
council, or one year of a council's files. See docs/document-pipeline.md.

    manage.py embeddings --council STO --dry-run
    manage.py embeddings --council STO --year 2026
    manage.py embeddings --file STO/<sha256>.pdf --embedder titan-v2
    manage.py embeddings --council STO --source s3 --stages extract,chunk
    manage.py embeddings --council STO --queue           # to the AWS pipeline
    manage.py embeddings --drain-queue <url>             # the Batch worker

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
from lgsf.embeddings import aws, pipeline, queue_worker
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
            help="The embedding model (default fake, which calls nothing; "
            f"with --queue, the one the Lambdas use: {aws.EMBEDDER} or "
            f"{aws.DEFAULT_EMBEDDER})",
        )
        parser.add_argument(
            "--source",
            choices=list(Source),
            type=Source,
            help="Where the documents are: the data directory (local, the "
            "default) or the bucket LGSF_S3_BUCKET and LGSF_S3_PREFIX name "
            "(s3, the default with --queue and --drain-queue)",
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
        parser.add_argument(
            "--queue",
            action="store_true",
            help="Send the files that need something to the AWS pipeline's "
            f"documents queue ({aws.DOCUMENTS_QUEUE_URL}) instead of running "
            "them here",
        )
        parser.add_argument(
            "--max-queued",
            type=int,
            default=2000,
            help="With --queue, wait while the documents queue holds more "
            "than this many messages (default 2000)",
        )
        parser.add_argument(
            "--drain-queue",
            metavar="URL",
            help="Run as the Batch worker: extract and chunk the files on "
            "this SQS queue until it's empty, sending each to the embed "
            f"queue ({aws.EMBED_QUEUE_URL})",
        )

    def write(self, text=""):
        self.console.print(text, markup=False, highlight=False, soft_wrap=True)

    def handle(self, options):
        self.options = options
        in_aws = options["queue"] or options["drain_queue"]
        if options["source"] is None:
            options["source"] = Source.S3 if in_aws else Source.LOCAL
        if options["embedder"] is None:
            options["embedder"] = aws.embedder_name() if in_aws else "fake"
        pipeline_options = self.pipeline_options()
        if options["drain_queue"]:
            if options["council"] or options["file"] or options["year"]:
                self.parser.error(
                    "--drain-queue works on the queue's files; it can't go "
                    "with --council, --file or --year"
                )
            self.drain(store_for(options["source"]), pipeline_options)
            return
        if options["file"] is None and not options["council"]:
            self.parser.error("one of --council or --file is required")
        if options["file"] is not None and options["year"] is not None:
            self.parser.error(
                "--year chooses a council's files; it can't go with --file"
            )
        if options["queue"] and (options["force"] or options["retry_failed"]):
            self.parser.error(
                "--force and --retry-failed can't go with --queue: the "
                "pipeline plans each file from its status; to do a file "
                "again, delete its status.json"
            )

        store = store_for(options["source"])
        files = self.get_files(store)
        embedders = []
        if Stage.EMBED in options["stages"] and not options["queue"]:
            # Planning needs only the embedder's name; the Lambdas embed.
            embedders = [EMBEDDERS[options["embedder"]]()]

        names = [embedder.name for embedder in embedders] or [options["embedder"]]
        if len(files) == 1:
            plans = [pipeline.plan(store, files[0], pipeline_options, names)]
        else:
            plans = pipeline.plan_files(store, files, pipeline_options, names)
        self.output_plan(files, plans, store)
        if options["queue"]:
            self.queue(store, files, plans)
            return
        if options["dry_run"]:
            return
        self.run(store, files, plans, embedders, pipeline_options)

    def pipeline_options(self):
        options = self.options
        return pipeline.Options(
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

    def queue(self, store, files, plans):
        """
        --queue: the files that need something, to the documents queue,
        leaving out those already waiting on the extract queue.
        """
        to_queue = [
            file for file, p in zip(files, plans, strict=True) if not p.nothing_to_do
        ]
        waiting = aws.recently_queued_files(store, to_queue)
        to_queue = [file for file in to_queue if file not in waiting]
        if waiting:
            self.write(f"\n{len(waiting)} files already queued for extraction")
        self.write(f"\n{len(to_queue)} files to queue")
        if self.options["dry_run"] or not to_queue:
            return
        sent = aws.queue_backfill(
            to_queue, max_queued=self.options["max_queued"], log=self.write
        )
        self.write(f"{sent} files queued")

    def check_docling(self):
        if importlib.util.find_spec("docling") is None:
            self.parser.error(
                "docling isn't installed. Run: uv sync --group embeddings"
            )

    def run(self, store, files, plans, embedders, pipeline_options):
        options = self.options
        to_convert = [p for p in plans if p.needs_converter]
        if to_convert:
            self.check_docling()

        self.to_do = sum(not p.nothing_to_do for p in plans)
        executor = (
            worker_pool(options["workers"], pipeline_options.converter)
            if to_convert
            else contextlib.nullcontext()
        )

        def run_files():
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

        self.run_reporting(run_files)

    def drain(self, store, pipeline_options):
        """--drain-queue: the Batch worker."""
        self.check_docling()
        self.to_do = None
        self.write(f"Working {self.options['drain_queue']} against {store!r}")
        self.run_reporting(
            lambda: queue_worker.drain(
                self.options["drain_queue"],
                store,
                pipeline_options,
                workers=self.options["workers"],
                on_outcome=self.output_outcome,
                log=self.write,
            )
        )

    def run_reporting(self, run):
        """
        Call ``run``, which reports each file through output_outcome(),
        and print the summary, whether it finished or was stopped.
        """
        self.outcomes = []
        self.write()
        start = time.monotonic()
        stopped = None
        try:
            run()
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
        # The worker doesn't know how many files its queue holds.
        count = f"{len(self.outcomes)}"
        if self.to_do is not None:
            count += f"/{self.to_do}"
        self.write(
            f"[{count}] {outcome.file.council} {outcome.file.storage_key}: "
            + "; ".join(parts)
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
