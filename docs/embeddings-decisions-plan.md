# Plan: embeddings of 2026 decisions and their documents

## Overview

Extend `lgsf/embeddings/` to make embeddings of council decisions taken in
2026, and of the PDFs attached to them, for the Welsh and Greater Manchester
councils listed in `sync_council_data.py`.

There are two phases:

1. **Decision JSON.** A `DecisionChunker` reads `Decisions/json/`. Nothing new
   needs installing, and the output goes through the existing chunk, embed,
   load and search steps unchanged.
2. **Attached PDFs.** A new `extract` step converts each PDF with Docling once
   and saves the result. A `DocumentChunker` turns the saved results into
   chunks, which then go through the same embed, load and search steps.

Limiting this to 2026 keeps phase 2 to about 109,000 pages. Docling takes
about 28 hours to get through that on one process on this machine. All the
decisions held link to about 1.1 million pages.

## Scope

**In:**

- the 32 councils in `sync_council_data.py`, of which 26 have 2026 decisions
- decisions whose `raw_date`, the date the decision was taken, is in 2026
- the PDFs those decisions link to
- `--since` and `--until` options, so the date range isn't written into code

**Not in:**

- decisions before 2026, minutes, and minutes PDFs. Extracted documents are
  looked up by content hash, so any of these can be added later without
  redoing work.
- OCR beyond what Docling does by default, which is to OCR regions of a page
  that have no text layer
- the vector index and binary embedding files that a million or more chunks
  would need
- detecting Welsh-language text

## The data this is based on

Counted from `data/` on 14 October 2026, after syncing from S3.

**Decisions dated 2026:**

- 5,331 decisions from 26 councils. The most are FLN (675), PEM (629) and NTL
  (614). MAN has 88. BGE and POW have one each and NWP has three.
- None from BGW, BOL, CAY, CGN, TOF or VGL. Nothing has been synced for VGL,
  whose decisions scraper was only added in 0c6173b.
- 4,606 have a `purpose` or `text`. 561 have procedural titles (apologies,
  declarations of interest, minutes, urgent business). About 270 have more
  than 350 words. There are under 480,000 words in all.
- 294 decisions taken before 2026 were published in 2026. Filtering on
  `raw_date` leaves them out.

**The documents they link to:**

- 9,205 unique files by `content_hash`, 6.8 GB, all on disk in
  `Decisions/documents/`.
- 593 of them aren't PDFs, although most are stored with a `.pdf` name. They
  are ModernGov "Reasons restricted" pages and error pages, saved as HTML.
  They're found by checking whether the file starts with `%PDF-`.
- 8,612 PDFs, with 109,338 pages between them. The median is 5 pages and the
  90th percentile 26. 33 have more than 200 pages; the longest, from TRF, has
  1,314.
- 432 PDFs give `pdftotext` fewer than 30 words per page, so are probably
  scans.
- `pdftotext` gets 31 million words out of them.

Mansfield (MAS) also has 2026 decisions in `data/`, from an earlier sync, but
not their documents. Naming the councils with `--council` leaves it out.

**Docling, measured on this machine** (32 cores, no GPU), on 16 decision PDFs
picked at random, 150 pages in all:

- 0.92 seconds per page on average with 8 threads, ranging from 0.29 to 1.91.
  Pages with tables are the slowest.
- Four processes with 8 threads each managed 1.38 seconds per page between
  them, which is slower than one process. The load average reached 48, so they
  were starting more threads than the 8 they were given. Each process used
  about 5.4 GB of memory.
- `pdftotext` takes 2 ms per page.
- On a scanned Bury officer decision form, `pdftotext` got no text and Docling
  got all of it, through OCR.
- Docling keeps table rows together and drops page numbers. Across the 16
  files, 370 lines of `pdftotext` output were only a number; 1 line of
  Docling's was.

## Phase 1: decision JSON

### `models.py`

- `ChunkType.DECISIONS = "decisions"`, plural to match
  `ChunkType.MINUTES = "minutes"` and the `Decisions/` directory. Chunks are
  written to `Embeddings/decisions.jsonl`.
- `SourceType.DECISION_JSON = "decision_json"`. Every decision scraper writes
  the same `DecisionBase` JSON, whatever the CMS, so there is one source type
  and one chunker.

### `DecisionChunker` in `chunkers.py`

- `has_data()`: `Decisions/json/` exists.
- `get_chunks()`: one source per decision JSON whose `raw_date` is in range.
  `sources_read` counts the decisions read.
- `parent_id` is `{council}:decisions:{raw_identifier}`. Text is split with
  `split_text()` at 350 words, the same as minutes.
- The text is `purpose` followed by `text`, each as its own paragraph, leaving
  out either if it's empty. The scraper already writes both as plain text with
  a blank line between paragraphs, so `html_to_text()` isn't needed.
- The context header, for `data/MAN/Decisions/json/2026-08-06-1172.json`:

  ```
  Manchester City Council · Strategic Director (Corporate Services) & City Treasurer · 6 August 2026
  Decision: P-1475 Domestic Abuse - Intersecting Needs Safe Accommodation (2026/06/09A)
  ```

- `date` is `raw_date`.
- `embed` is false for procedural titles. Decisions with no `purpose` or
  `text` are embedded anyway: their title and decision maker are all they have
  until phase 2 adds their documents. Check both rules once the output can be
  read.
- `metadata`:
  - `url`: the decision page
  - `title`, `decision_maker`, `status`, `publication_date`
  - `is_key_decision`, `is_officer_decision`, `is_subject_to_call_in`
  - `document_hashes`: the `content_hash` of each linked document, in order.
    This connects a decision to its document chunks in phase 2, as
    `linked_attachment_ids` does for minutes.

### `manage.py embeddings chunk`

- Run every chunker that has data for a council. At the moment one chunker is
  chosen from `services.minutes.cms_type`. Minutes keep that lookup;
  decisions and documents don't need one.
- `--type minutes,decisions,documents` chooses which chunkers run. Each type
  writes its own file, so the file of a type that isn't run is left alone.
- `--since` and `--until` are passed to every chunker. `ChunkerBase` keeps
  them, with an `in_range(date)` method, and the minutes chunker uses it too.
- The summary gets a type column.

### `search.py` and `manage.py embeddings search`

- Results show `metadata["meeting_url"]`. Instead, every chunker writes
  `metadata["url"]`, the page to open, and search shows that. The minutes
  chunker writes it too. Metadata isn't part of `text_hash`, so nothing needs
  embedding again.
- Add `--type` to filter on `chunk.type`, through `SearchFilters`.
- One result per source already groups by `parent_id`, which gives one result
  per decision.

`load.py` doesn't change. It loads every `Embeddings/*.jsonl` file, and the
`chunk` table already has a `type` column.

### Tests

Fixtures are decision JSON files from MAN and PEM: one with both `purpose`
and `text`, one with neither, and one with a procedural title. Test the chunk
IDs, the header, the order of the text, the `embed` rules, the date range and
the metadata, plus `chunk --type` and `--since`.

### Running it

```
COUNCILS=AGY,GWN,CWY,DEN,FLN,WRX,CGN,PEM,CMN,SWA,NTL,BGE,VGL,CRF,RCT,CAY,BGW,TOF,MON,NWP,POW,MTY,BOL,BUR,MAN,OLD,RCH,SLF,SKP,TAM,TRF,WGN

python manage.py embeddings chunk --type decisions --since 2026-01-01 --council $COUNCILS
AWS_PROFILE=dev-lgsf-dc python manage.py embeddings embed --model titan-v2 --council $COUNCILS
python manage.py embeddings embed --model bge-m3 --council $COUNCILS
uv run lgsf/embeddings/db/load.py --data-dir data --model titan-v2 --model bge-m3
python manage.py embeddings search --model titan-v2 --type decisions "safe accommodation for domestic abuse"
```

There are about 5,000 chunks to embed: 5,331 decisions, less the 561
procedural ones, plus at least one more for each of the 270 that are split.
At the rates in the README, Titan takes about 2 minutes and bge-m3 about 20.

## Phase 2: attached PDFs

This is split into two steps, because Docling is slow and the chunking rules
will change more than once:

1. `extract`: Docling converts each PDF once, and the result is saved as JSON.
   This takes hours and is run once.
2. `chunk --type documents`: reads the saved JSON. This takes minutes and is
   run as often as needed.

### Dependencies

Add `docling` to the `embeddings` dependency group, with `torchvision` taken
from the PyTorch CPU index, as `torch` already is. The group already has
`torch` and `transformers`; `HybridChunker` needs `docling-core[chunking]`,
which brings in `transformers`. A trial environment with docling 2.138.0 and
CPU torch came to 1.4 GB.

On its first run Docling downloads its layout model
(`docling-project/docling-layout-heron`) and TableFormer
(`docling-project/docling-models`) from Hugging Face, and RapidOCR's PP-OCRv6
models from modelscope.cn.

### `extract.py` and `manage.py embeddings extract`

```
python manage.py embeddings extract --council MAN --since 2026-01-01 --workers 4 --threads 4
```

- It finds the documents linked from decisions in the date range, using the
  same filter as `chunk`, and removes duplicates by `content_hash`. 324 of the
  9,198 files linked from 2026 decisions are linked under more than one
  storage key (623 extra keys), mostly by GWN attaching the same PDF to a
  later decision. Each is extracted once.
- It skips files that don't start with `%PDF-`, logging them as `not_pdf`.
- Each result is written with `DoclingDocument.save_as_json()` to
  `data/<COUNCIL>/Embeddings/extracted/<storage key>.json`, with the storage
  key's extension left off. For example,
  `Decisions/documents/2026-08-06-1172-s65106.pdf` is extracted to
  `extracted/2026-08-06-1172-s65106.json`, so either can be found from the
  other. Storage keys are unique within a council, and are at most 32
  characters.

  Document titles aren't used in file names because they repeat: 895 of
  these files are titled "View reasons restricted", 213 "Appendix A" and 114
  "Report". Some also contain `/`, as in "2026/27".

  A file linked under several storage keys is named after the key of the
  earliest decision linking to it, so the name doesn't depend on the order
  files are run in. The other keys find it through the log.

  `embed` only reads `Embeddings/*.jsonl` and the directory of the model it's
  given, so this directory doesn't get in its way. No model can be called
  `extracted`.
- Each file gets a line in `Embeddings/extracted/log.jsonl`:
  - `content_hash`, `storage_key`, `pages`, `seconds`
  - `file`: the name the JSON was written to, or empty if none was. The
    chunker reads this rather than working the name out from `storage_key`.
  - `status`: `ok`, `partial`, `failed`, `timeout` or `not_pdf`
    (`ConversionStatus` has `SUCCESS`, `PARTIAL_SUCCESS` and `FAILURE`).
    `partial` results are saved and chunked.
  - `error`
  - the Docling version, so it's clear later which output came from which
    version
- The log, not the directory, records what has been extracted. `extract`
  and `DocumentChunker` both use it to find the file for a `content_hash`:
  - Lines are only ever added, so a file can have several, such as `failed`
    and then `ok`. The last line for a `content_hash` is the one that counts.
  - Each worker writes its JSON to a temporary file and renames it when it's
    complete. The worker then hands its result to the parent process, which
    adds the log line. Workers don't write to the log, so lines from
    different processes can't be mixed together.
  - A JSON file with no line in the log is ignored, and the next run
    extracts it again. A run that is stopped leaves at most one of these per
    worker. Temporary files left by a stopped run are deleted at the start
    of the next.
  - A run skips files whose last line is `ok`, `partial` or `not_pdf`, so it
    can be stopped and started again. It also skips `failed` and `timeout`,
    unless given `--retry-failed`, so a PDF that times out doesn't use up its
    `document_timeout` again on every run. `--force` extracts everything
    again.
  - The same file, linked from minutes later on, is found by its
    `content_hash` and reused.
- Docling's default pipeline: layout model, TableFormer, and OCR of regions
  without a text layer. Each file has a `document_timeout`.
- Files are converted in a process pool. Each worker sets `OMP_NUM_THREADS`,
  `MKL_NUM_THREADS` and `torch.set_num_threads()` before loading the models,
  then loads the converter once.
- Smallest files go first. The 33 PDFs of more than 200 pages run last, so a
  problem with one of them doesn't hold up the rest.
- The JSON for a 2-page MAN PDF was 49 KB, about 25 KB per page. That suggests
  about 3 GB for 109,000 pages. Check this on the first council.

### Tuning before the full run

Time about 300 pages with each of: 1 process × 8 threads, 2 × 8, 4 × 4,
8 × 2 and 16 × 1, with the thread variables set. Keep whichever gets through
the most pages per second. If none beats 0.92 seconds per page, the full run
takes about 28 hours.

### `DocumentChunker` in `chunkers.py`

- `ChunkType.DOCUMENTS = "documents"`, `SourceType.PDF_DOCLING = "pdf_docling"`
- It reads the extracted JSON of documents linked from decisions in range,
  finding each one's file by `content_hash` in `extracted/log.jsonl`. Linked
  documents whose last log line isn't `ok` or `partial`, or that have no
  line, are counted in the summary.
- Text is split with docling-core's `HybridChunker`. It splits at the section
  headings Docling found, then by size, and merges small neighbouring pieces
  that share a heading. Its tokenizer counts words, with a limit of 350, the
  same as `split_text()`, until an embedding model is chosen (open decision 1
  in `embeddings-plan.md`).

  Tried on `MAN/Decisions/documents/2026-08-06-1172-s65106.pdf`, it gave 4
  chunks, each with its page numbers and heading.
- `HybridChunker` writes a table one row at a time, as `row label, column
  label = value`. When a table has no header row, Docling names the columns 1,
  2 and so on, which gives `Decision maker, 1 = City Treasurer…`. Write a
  table serializer that leaves out numbered column labels, giving `Decision
  maker: City Treasurer…`.
- `parent_id` is `{council}:documents:{sha256}`.
- `date` is the date of the earliest decision in range that links to the
  document.
- The context header names the decision, then the document and the section:

  ```
  Manchester City Council · Strategic Director (Corporate Services) & City Treasurer · 6 August 2026
  Decision: P-1475 Domestic Abuse - Intersecting Needs Safe Accommodation (2026/06/09A)
  D3 P-1475 Domestic Abuse - Intersecting Needs Safe Accommodation · Notice of Decision Made
  ```

  If several decisions link to the document, the header names the first and
  the metadata lists them all.
- `embed` is false under 15 words, as for minutes. The first chunk of the MAN
  example was 8 words: "Made by an Executive Member or Chief Officer".
- `metadata`:
  - `url`: the document
  - `content_hash`, `storage_key`, `document_title`
  - `pages`, `headings`
  - `decisions`: the `identifier`, `title`, `url` and `date` of each decision
    linking to it

Search shows the decision as well as the document.

### Size

- **Extraction:** about 28 hours at 0.92 seconds per page, or less after
  tuning.
- **Chunks:** at least 90,000 (31 million words at 350 words per chunk). There
  will be more, because chunks also end at headings.
- **Titan:** about 35 minutes. It should cost under $1: about 40 million
  tokens at $0.02 per million. Check the eu-west-2 price.
- **bge-m3:** about 7 hours.
- **Embedding files:** about 2 GB per model as JSON lines, which is
  manageable at this size.
- **Postgres:** about 100,000 rows with no index, so every search compares
  every row. Check how long a search takes, and add an HNSW index only if it's
  too slow.

## Order of work

Each step is its own commit, or several:

1. Rebase `embeddings` onto master and push it. It's only on this machine at
   the moment.
2. `chunk` runs every chunker type, with `--type`, `--since` and `--until`.
3. `DecisionChunker` and its tests.
4. `url` in chunk metadata, and `--type` for search.
5. Run phase 1 and check it (below).
6. Add `docling` to the `embeddings` group.
7. The `extract` command, with tests of choosing, skipping and logging files
   that don't load Docling's models. They cover naming a file linked under
   several storage keys, the last log line counting, a JSON file with no log
   line being extracted again, and `--retry-failed`.
8. Tune workers and threads. Extract MAN first (89 PDFs, 190 pages), then the
   rest.
9. `DocumentChunker`, the table serializer, and tests. The fixture is a saved
   DoclingDocument JSON, so the tests don't need Docling's models either.
10. Chunk, embed, load and check.
11. Update `lgsf/embeddings/README.txt`.

## Checking it

**Phase 1:**

- Read 20 chunks picked at random from `decisions.jsonl` alongside their
  decision pages.
- Read the list of decisions marked `embed: false`, looking for any that
  aren't procedural.
- Before searching, write down 20 questions whose answer you know, for
  example "safe accommodation for domestic abuse in Manchester". Note where
  the decision you expect comes in the results for each model.

**Phase 2:**

- Count the extraction log by status. Look at the failures, the timeouts, and
  files with pages but almost no text after OCR.
- Compare 10 document chunks picked at random with the pages they cite.
- Ask the 20 questions again, with and without `--type decisions`. Look
  especially at whether document chunks find decisions that have no text of
  their own, which is most of BUR's, RCT's and WGN's.
- Optionally, extract MAN with `pdftotext` too, and compare the search results
  of the two.

## Open decisions

1. **What "2026" means.** This plan uses `raw_date`, the date the decision was
   taken. Using `publication_date` would add 294 decisions.
2. **Embedding decisions that have only a title.** Check the output first.
3. **Chunk size for documents.** 350 words is the setting for minutes, and
   reports may suit longer chunks. Titan v2 and bge-m3 each accept up to 8,192
   tokens.
4. **The 33 PDFs of more than 200 pages.** Look at what they are before
   deciding whether they're worth the time.
5. **Which model to keep**, once the questions have been asked of both.
