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
