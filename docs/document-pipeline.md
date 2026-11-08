# The document pipeline

`manage.py embeddings` turns the documents the scrapers store into text,
chunks and embeddings, and writes the results beside each document. The
code is in `lgsf/embeddings/`.

## What it's for

Scrapers store a council's documents (agenda packs, reports, minutes,
decision notices) as files named after their content, at
`<COUNCIL>/documents/<sha256><ext>`, locally under `data/` and in S3 under
`LGSF_S3_PREFIX`. The pipeline reads each file once, extracts its text with
[Docling](https://docling-project.github.io/docling/), splits the text into
chunks of up to 350 words, embeds the chunks, and writes five files next to
the document. Anything that wants a document's text or embeddings reads
those files; nothing has to run Docling again.

**The unit of work is a file**: a council code and a storage key. The
pipeline doesn't know which meeting or decision links a file, and nothing
in its outputs names one. A file linked from ten meetings is done once,
and its results don't change when a meeting is retitled or a decision is
re-imported. The records that link the file (`<COUNCIL>/Minutes/json/…`,
`<COUNCIL>/Decisions/json/…`) carry the `storage_key`, so a reader joins the
two.

## The five files

For `STO/documents/<sha256>.pdf`:

| File | Holds |
| --- | --- |
| `<sha256>.pdf.docling-v2.json` | Docling's document JSON, with images as placeholders. The `v2` is Docling's major version; the exact version is in the status. |
| `<sha256>.pdf.md` | The text as Markdown (`export_to_markdown`). |
| `<sha256>.pdf.chunks.jsonl` | One chunk a line. |
| `<sha256>.pdf.titan-v2.jsonl` | One embedding a line, named after the embedder (`<embedder>.jsonl`; the fake embedder writes `.fake.jsonl`). Only chunks with `embed: true` have one. |
| `<sha256>.pdf.status.json` | One object saying what has been done. Written last. |

A file that isn't a document to extract (see outcomes below) gets only the
status file.

In AWS, dispatch also writes `<sha256>.pdf.queued.json` beside a file
it sends to the extract queue (see "Running in AWS"). It's not one of
the pipeline's results and not part of the schema; readers ignore it.

JSON Schemas for the chunks, embeddings and status files are in
`lgsf/embeddings/schemas/v1/`. They allow fields to be added without a new
version; a change to what a field means needs one.

### A chunk

Serialised with `sort_keys=True, ensure_ascii=False`.

| Field | |
| --- | --- |
| `chunk_id` | `{council}:documents:{sha256 hex}:{chunk_index}`, stable between runs |
| `council` | The council code |
| `content_hash` | `sha256:<hex>`, as records have it |
| `storage_key` | `<hex><ext>` |
| `chunk_index`, `chunk_count` | This chunk's place, and how many there are |
| `header` | The document's title and the chunk's section headings (below) |
| `text` | The chunk's text |
| `headings` | The section headings, outermost first |
| `pages` | The pages the text is on, sorted |
| `embed` | `false` when the text has fewer than 15 words, which is usually a line on its own |
| `chunker_version` | The version of the chunking code, currently 1 |
| `word_count` | Words in `text` |
| `text_hash` | SHA-256 hex of the text that is embedded: `header + "\n\n" + text` when there is a header, else `text` |

Tables are written one row to a line. A table with no header row (a
decision notice's labels and values) is written as `Label: value`; one
with a header row as `row label, column label = value`.

### The header

The header puts the document in front of each chunk when it's embedded,
so the chunk can be found by what it's part of. It names the document and
the section only: no council, no meeting, no decision, no date.

- Line 1 is the document's title. That's the PDF's metadata Title if it
  has one worth using: a leading "Microsoft Word - " and a trailing
  ".docx" or ".doc" are stripped, and titles that are empty, "untitled",
  Word's "Document1" or a file name are ignored. Otherwise it's the first
  heading-like item Docling found (a title or section header). Otherwise
  there's no title line.
- Line 2 is the chunk's section headings joined with " · ". A heading that
  repeats the title isn't written twice.
- Empty lines are dropped, so the header can be empty.

The rule is `chunking.document_title()` and `chunking.header()`.

### An embedding

| Field | |
| --- | --- |
| `chunk_id` | As in the chunks file |
| `text_hash` | The chunk's `text_hash` when it was embedded |
| `model` | The provider's model ID, such as `amazon.titan-embed-text-v2:0` |
| `dimensions` | The vector's length |
| `token_count` | Tokens the model read, by its own tokenizer |
| `embedding` | The vector |

Embedders: `titan-v2` is Amazon Titan Text Embeddings V2 on Bedrock, 1024
normalised dimensions, called from eight threads with adaptive retries,
in the region the environment gives. `fake` makes four dimensions from a
hash of the text and calls nothing; it's the default, for running the
pipeline without AWS.

### The status

```json
{
  "schema_version": 1,
  "council": "STO",
  "storage_key": "<sha256>.pdf",
  "content_hash": "sha256:<hex>",
  "format": "pdf",
  "outcome": "ok",
  "error": null,
  "stages": {
    "extract": {"version": 2, "tool_version": "2.130.0", "finished_at": "…", "status": "ok", "pages": 1, "seconds": 6.2, "device": "cpu"},
    "chunk": {"version": 1, "finished_at": "…", "inputs_hash": "sha256:…", "chunks": 2, "embeddable": 2, "error": null},
    "embed": {
      "titan-v2": {"version": 1, "model": "amazon.titan-embed-text-v2:0", "finished_at": "…", "chunk_inputs_hash": "sha256:…", "embedded": 2, "reused": 0, "tokens": 512, "error": null}
    }
  }
}
```

A stage's entry is absent until the stage has run. `format` is what the
file's bytes say it is (`formats.py`): the storage key's extension says
less, since councils serve HTML error pages and Word files as `.pdf`.

`outcome` is one of:

| Outcome | When |
| --- | --- |
| `ok` | Extracted (Docling's status `ok` or `partial`; a partial conversion's message is in `error`) |
| `restricted` | ModernGov's "Reasons restricted" page, saved in place of a document the council hasn't published |
| `empty` | No bytes, or only zero bytes |
| `not_a_document` | Another HTML page (a logon or error page), or a format Docling isn't given (a ZIP that isn't an Office file, an old binary Word file, Rich Text) |
| `failed` | Docling failed or timed out, the file's SHA-256 isn't the one in its name, a status file that couldn't be parsed (`error` is "unreadable status"), or a stage is running again and has not finished (`error` is "superseded: <stage> running again"; see below). `error` says which. |

A chunking or embedding error (docling-core failing on a document, a
Bedrock throttle that outlasts the retries) is recorded in that stage's
`error`, the outcome stays `ok`, and the next run tries the stage again.
When some of a document's chunks embed and others don't, the embeddings
that succeeded are written, the entry's `error` says how many didn't,
and the next run sends only those.

A status whose fields the code doesn't know are dropped when it's read,
so fields can be added without a new schema version. A status that
can't be parsed at all counts as `failed`, so `--retry-failed` covers it
and Docling isn't run again by accident.

A status with an extract stage from Docling 2.130.0 and no chunk stage
may be one of lgsf-web's earlier conversions, copied into this layout by
`scripts/copy_lgsf_web_results.py` with its `.docling-v2.json` and `.md`;
the pipeline chunks and embeds such a file as it would any other
extracted one, without converting it again.

## When each stage runs again

Every run reads each file's status and runs only what's needed, so a run
that's stopped is picked up by the next, and a run over files already done
writes nothing. A status whose `content_hash` isn't the file's is treated
as missing: it's about other bytes.

- **Extract** runs when there's no extract entry, with `--force`, or with
  `--retry-failed` when the outcome was `failed`. A file that was extracted
  or left out isn't extracted again on its own: it takes a second a page
  on a CPU, and a file that timed out would take its whole timeout every
  run.
- **Chunk** runs when the file is extracted and the chunk entry's
  `inputs_hash` isn't the current one. The inputs are the chunker version,
  the word limits, the Docling version that converted the file, the file's
  format and its hash. Raising `CHUNKER_VERSION` in `records.py` chunks
  every document again without converting any.
- **Embed** runs when the file is chunked and the embedder's entry is
  missing or its `chunk_inputs_hash` isn't the chunk entry's. An embedding
  already in the file is reused when its `text_hash`, `model` and
  `dimensions` match the chunk's, so chunking again only embeds the
  chunks whose text changed. `reused` and `embedded` in the entry say how
  many each.
- **A stage whose input has gone runs again.** If a status says a file is
  extracted but its `.docling-v2.json` is missing or unreadable, the
  extract entry is dropped and the file is converted again; if it says
  chunked but `.chunks.jsonl` is missing or unreadable, the chunk entry is
  dropped and the file is chunked again. Missing files are noticed from
  the directory listing when a run is planned; unreadable ones when the
  stage reads them.

## Running it

Extracting and chunking need Docling, which is in its own dependency
group so that the Lambda layer (built from `uv export --no-dev`, and under
250 MB) doesn't carry it:

```
uv sync --group embeddings
```

That installs about 1.7 GB, torch most of it, from PyTorch's CPU-only
index. The first conversion downloads Docling's models (layout,
TableFormer, OCR) to the Hugging Face cache.

```
uv run manage.py embeddings --council STO --dry-run       # what would run
uv run manage.py embeddings --council STO                 # every file of a council
uv run manage.py embeddings --council STO --year 2026     # files linked from 2026's meetings and decisions
uv run manage.py embeddings --file STO/<sha256>.pdf       # one file
uv run manage.py embeddings --council STO --embedder titan-v2   # real embeddings; needs bedrock:InvokeModel
uv run manage.py embeddings --council STO --source s3     # against the bucket LGSF_S3_BUCKET/LGSF_S3_PREFIX
```

Other options: `--stages extract,chunk,embed` to run some stages,
`--force`, `--retry-failed`, `--max-pages N` to leave long PDFs for a later
run, `--limit N`, and for Docling `--device auto|cpu|cuda`, `--workers`
(processes, each with its own copy of the models), `--threads` a worker
and `--timeout` a file.

The command prints a line a file as it finishes, then counts. Stopping it
loses nothing. A file's status is written last, so a file with no status
is done next time. Before a stage overwrites files that a status already
vouches for (a re-chunk after a chunker version change, a `--force`
re-extract, embedding again), the status is written first with that
stage's entries removed and outcome `failed`, error "superseded: <stage>
running again"; then the files; then the final status. So a run stopped
in between leaves a status that says the stage still needs running, and
never one that vouches for files that don't match it. An error in the
store itself (rather than in Docling or the model) writes no status, is
reported for that file, and the run carries on; the next run tries the
file again.

Files that need Docling are submitted smallest first, by the listing's
sizes, so one slow agenda pack doesn't hold up the end of a run.

The library does the work without the command: `pipeline.run_file(store,
File(council, storage_key), Options(), [embedder])` runs every stage one
file needs in the current process, and `pipeline.run_files()` many, with
or without a pool of worker processes. Neither uses argparse or Rich, so a
Lambda or a Batch job can call them. The stores are `stores.LocalStore(root)`
and `stores.S3Store(bucket, prefix)`; the files are chosen by
`files.council_files()`, `files.files_for_year()` or `files.parse_file()`.

How Docling runs is on the options: `Options(converter=ConverterSettings(
threads=4, timeout=1800, device=Device.AUTO))`. `run_file()` and
`run_files()` configure the converter in their process from it; a pool
made with `extract.worker_pool(workers, settings)` configures each worker
the same way. The first conversion in a process fixes the settings.

`pipeline.plan_files(store, files, options, embedder_names)` plans many
files from one listing of each council's documents directory, reading
only the status files the listing has (in 16 threads) rather than making
a request for every file; `pipeline.plan()` reads one file's status.
`run_files()` plans this way itself when it isn't given plans.

A process without Docling installed can still settle most of a council's
files: `pipeline.dispatch(store, file)` downloads the file, and if it's
restricted, empty, not a document, or not the bytes its name says,
writes its status and returns the outcome; if it only needs embedding,
embeds it; otherwise it writes nothing and the outcome's
`needs_converter` is true, for a process that has Docling to pick up.
Everything in `lgsf/embeddings/` other than `documents.py` imports
without Docling, torch, pypdfium2 or pandas.

## Infrastructure

On AWS the pipeline runs in the LGSF stack, as three Lambda functions,
three SQS queues and an AWS Batch job queue, with an SNS topic for what
it finishes; `cdk/stacks/documents.py` makes them and
[cdk/README.md](../cdk/README.md) describes them. The handlers are
`lgsf/embeddings/aws.py` and the Batch worker `lgsf/embeddings/queue_worker.py`,
run by `manage.py embeddings --drain-queue`. The files are the ones above,
in the data bucket under `merged-data/`; nothing differs from a run on a
laptop with `--source s3` except who does which stage.

## Running in AWS

In AWS the same library runs as three Lambdas and a Batch job, joined by
SQS queues. A new document that turns out not to need Docling (a
restricted page, an empty file) has its status within minutes of being
scraped, and a chunked document its embeddings within minutes; a
document that needs extracting waits for the hourly Batch submission
and the instance to start, so is typically done within an hour or two,
longer while a backlog converts. A backfill of every council's
documents runs alongside without holding the new ones up. The code
is in `lgsf/embeddings/aws.py` (the Lambdas, the messages, "file ready")
and `lgsf/embeddings/queue_worker.py` (the Batch worker); the library
is unchanged. The Lambdas run with boto3 alone: `aws.py` imports without
Docling.

Every message on every queue is one file, as JSON:

```json
{"council": "STO", "storage_key": "<sha256>.pdf"}
```

Messages are sent ten a request, the most SQS takes. A message carries
no instructions: each stage plans the file from its status, as a run
does, so a message delivered twice costs a status read and nothing else.

A file can wait on the extract queue for hours while a backlog
converts, with no status yet. So that the next scrape's trigger, a
sweep or a backfill doesn't queue it again meanwhile, dispatch writes a
marker beside it when it sends it there, `<key>.queued.json`
(`{"queued_at": "<ISO UTC>", "queue": "extract"}`), and the trigger
and `--queue` skip a file whose marker is younger than seven days. An
older marker is ignored, so a message that was lost is made up for by
a later sweep (the extract queue keeps a message for fourteen days).
Nothing removes the marker: once the status is written the file is
skipped anyway, and the marker is tiny.

### The queues

| Queue | Variable | Holds |
| --- | --- | --- |
| documents | `DOCUMENTS_QUEUE_URL` | Files that may need something, from the trigger and from `--queue` |
| extract | `EXTRACT_QUEUE_URL` | Files that need Docling: the Batch worker's work |
| embed | `EMBED_QUEUE_URL` | Files that are chunked and need embedding |

### The trigger (`lgsf.embeddings.aws.trigger_handler`)

Finds files for the pipeline to do. It tells the event it was given
apart by looking at it:

- **A message from the `data-commits` SNS topic** (see
  `lgsf/storage/doorbell.py`), after a council's scrape has pushed: it
  lists the council's documents directory once and sends every document
  that has no `.status.json` to the documents queue. A direct invocation
  with `{"council": "STO"}` does the same. A file with a fresh
  `.queued.json` (above) is left out, and so is one written to the
  bucket before `DOCUMENTS_SINCE` (an ISO UTC timestamp, such as
  `2026-11-09T00:00:00Z`): the trigger is for what the scrapers add from
  now on, and the historical files, about a million of them, come in
  through the backfill command at its own pace. Without the variable the
  trigger queues nothing and logs a warning, so a sweep of the whole
  bucket can't happen by accident.
- **A sweep**, `{"sweep": true}` or EventBridge's "Step Functions
  Execution Status Change" event when the scraping state machine
  finishes: it lists the councils (the directories under the prefix) and
  invokes itself asynchronously once a council (`TRIGGER_FUNCTION_NAME`),
  so no one invocation runs long.
- **Submit a Batch job**, `{"submit_batch": true}` or EventBridge's
  scheduled event: if the extract queue has messages waiting and no job
  on the Batch job queue (`DOCUMENTS_BATCH_JOB_QUEUE`) is submitted,
  pending, runnable, starting or running, it submits one
  (`DOCUMENTS_BATCH_JOB_DEFINITION`), named `documents-<UTC timestamp>`.
  One job at a time: the worker drains the queue whatever its length.

It returns a small dict saying what it did.

### Dispatch (`lgsf.embeddings.aws.dispatch_handler`)

An SQS event source on the documents queue, up to ten messages at a
time, with `ReportBatchItemFailures`. For each file it calls
`pipeline.dispatch()`, which settles without Docling what it can:

- a restricted page, an empty file, a page that isn't a document, or a
  file whose bytes aren't its name's: the status is written and "file
  ready" is published;
- a file that needs Docling, to extract or to chunk: sent to the extract
  queue, and marked with `.queued.json`;
- a file that's chunked but not embedded by the Lambdas' embedder, or
  whose embedding has an error (some chunks not embedded): sent to the
  embed queue;
- a file that's done: "file ready" is published again, so a subscriber
  that missed it can ask for a file by queueing it.

A record whose file isn't in the store, or that the store failed on, is
returned as a batch item failure, so SQS delivers it again and in the
end moves it to the dead-letter queue.

### The Batch worker (`manage.py embeddings --drain-queue <url>`)

Runs on a Batch instance with Docling installed and
`EMBED_QUEUE_URL` and `FILE_READY_TOPIC_ARN` set. It receives up to ten
messages at a time with long polling, plans their files from their
statuses, and runs the extract and chunk stages for them with
`pipeline.run_files()` and a pool of `--workers` processes, as a run
does. While a batch converts, the messages' visibility is extended
every few minutes, so a long agenda pack doesn't reappear on the queue
before it's done. Then each message is settled by what became of its
file:

- chunked: the file is sent to the embed queue, and the message deleted;
- left out or failed (restricted, empty, not a document, Docling failed
  or timed out): "file ready" is published, and the message deleted;
- anything else (the store failed, the file isn't there, chunking
  failed): the message is left, so SQS delivers it again and in the end
  dead-letters it.

A file is sent on before its message is deleted, so a worker stopped
between the two repeats the file rather than losing it. The worker
prints a line a file as the command does, and exits when the queue has
been empty for two receives. `--council`, `--file` and `--year` don't go
with it; `--workers`, `--threads`, `--timeout` and `--device` do.

### Embed (`lgsf.embeddings.aws.embed_handler`)

An SQS event source on the embed queue, up to ten messages at a time,
with `ReportBatchItemFailures`. For each file it runs the embed stage
alone with the embedder `EMBEDDER` names (`titan-v2` unless set), then
publishes "file ready". It can send a file back: one whose status says
it needs Docling, or whose chunks turn out to have gone, goes to the
extract queue, marked as dispatch would mark it, and the record
succeeds. A file whose embed entry records an error ("N of M chunks not
embedded", a Bedrock throttle that outlasted the retries) is a batch
item failure, so SQS delivers it again and the next run sends only the
chunks that failed.

### "File ready" (`FILE_READY_TOPIC_ARN`)

Whenever a file's status reaches a final outcome (`ok` with its
embeddings written, `restricted`, `empty`, `not_a_document` or
`failed`), one message goes to the topic:

```json
{
  "council": "STO",
  "storage_key": "<sha256>.pdf",
  "content_hash": "sha256:<hex>",
  "outcome": "ok",
  "status_key": "STO/documents/<sha256>.pdf.status.json",
  "schema_version": 1
}
```

It's a notice, not a delivery: a subscriber reads the status and the
files beside the document from the bucket. It can arrive more than once
for a file. The one publisher is `Targets.ready()` in `aws.py`; without
the variable it logs a warning and sends nothing, so a worker on a
laptop stays quiet.

### Backfilling

```
uv run manage.py embeddings --council STO --queue --dry-run
uv run manage.py embeddings --council STO --queue
uv run manage.py embeddings --council STO --year 2025 --queue --max-queued 500
```

`--queue` plans the files as a run would, against the bucket, and sends
those that need something, and aren't already waiting on the extract
queue (a fresh `.queued.json`), to the documents queue, ten a request,
from where dispatch routes them. `DOCUMENTS_SINCE` doesn't apply: this
is how the files from before it get in, a council or a year at a time. While the documents queue holds more than
`--max-queued` messages (2,000 unless set) it waits, so a backfill never
swamps the files new scrapes add. The embedder it plans for is the one
the Lambdas use (`EMBEDDER`, or `titan-v2`), and `--source` is `s3`,
unless either is given. `--force` and `--retry-failed` don't go with it:
the Lambdas plan each file from its status, so to do a file again,
delete its `.status.json` and queue it.
