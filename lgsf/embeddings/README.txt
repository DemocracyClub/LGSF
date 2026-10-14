Embeddings
==========

This package turns scraped council data into chunks of text and creates
embeddings (vectors) of them, which can then be loaded into Postgres with
pgvector and searched by similarity. It handles three types of data:

  minutes     minutes from ModernGov's meeting XML
  decisions   the JSON the decisions scraper writes for each decision
  documents   the PDFs linked from decisions, converted by Docling

There are three steps. Each reads the files the step before it wrote:

  1. python manage.py embeddings chunk
                                  chunks each type of data into
                                  data/<COUNCIL>/Embeddings/<type>.jsonl
  2. python manage.py embeddings embed
                                  embeds those chunks into
                                  data/<COUNCIL>/Embeddings/<model>/
  3. uv run lgsf/embeddings/db/load.py
                                  loads chunks and embeddings into Postgres

Then `python manage.py embeddings search` searches them with a question.

Documents need a step before step 1, which converts the PDFs once and
takes hours:

  0. python manage.py embeddings extract
                                  converts the PDFs linked from decisions
                                  into data/<COUNCIL>/Embeddings/extracted/

Nothing is fetched from council websites. These steps read the minutes,
decisions and documents already in data/<COUNCIL>/Minutes/ and
data/<COUNCIL>/Decisions/, so run the scrapers (python manage.py minutes,
python manage.py decisions) or python manage.py sync first.

More detail, and the reasons for the design, are in
docs/embeddings-plan.md (chunking), docs/embeddings-creation-plan.md
(embedding and loading) and docs/embeddings-decisions-plan.md (decisions
and their documents).


Setup
-----

Steps 0 and 2, chunking documents, and searching need packages that
aren't installed by default:

  uv sync --group embeddings

This installs boto3, psycopg, sentence-transformers, Docling, and
CPU-only builds of torch and torchvision. They're kept out of the default
install, and out of the Lambda layer, because torch is large. A plain
`uv sync` removes them again, so run `uv sync --group embeddings` after
one.

The commands below are run from the repository root with the virtualenv
activated (source .venv/bin/activate).


0. Extract documents
--------------------

  python manage.py embeddings extract --council MAN --since 2026-01-01
  python manage.py embeddings extract --council MAN,PEM --since 2026-01-01 --workers 2 --threads 8

Converts each PDF linked from a decision dated between --since and
--until with Docling, and saves the result as JSON for step 1. Docling
takes about a second a page, so this is run once, and step 1 is run as
often as the chunking changes.

For each council it writes:

  data/<COUNCIL>/Embeddings/extracted/<storage key>.json
  data/<COUNCIL>/Embeddings/extracted/log.jsonl

- A file is named after the key it's stored under in Decisions/documents/:
  2026-08-06-1172-s65106.pdf is extracted to 2026-08-06-1172-s65106.json.
- A file linked from several decisions, under a different key each time,
  is extracted once, and named after the key of the earliest decision
  linking to it.
- Docling's default pipeline is used: its layout model, TableFormer for
  tables, and RapidOCR for regions of a page with no text layer. The
  first run downloads their models from Hugging Face and modelscope.cn.
- Files that don't start with %PDF- aren't sent to Docling. They're
  mostly ModernGov "Reasons restricted" pages saved with a .pdf name.

log.jsonl gets a line for each file each time it's tried:

  {"content_hash": "sha256:98e2...", "storage_key": "2026-08-06-1172-s65106.pdf",
   "file": "2026-08-06-1172-s65106.json", "status": "ok", "pages": 2,
   "seconds": 16.0, "error": null, "docling_version": "2.130.0"}

status is one of ok, partial (some pages failed), failed, timeout or
not_pdf. The log, not the directory, records what has been extracted:
the last line for a content_hash is the one that counts, and a JSON file
with no line in the log is ignored.

Runs carry on from each other:

- A run skips files whose last line is ok, partial or not_pdf, so it can
  be stopped with Ctrl-C and started again.
- It skips files that failed or timed out too, unless given
  --retry-failed. --force extracts everything again.
- Linked files that aren't on disk are counted, but not logged, so
  they're looked for again next time.

Options:

  --workers 4      how many PDFs to convert at once, each in its own
                   process with its own copy of Docling's models, which
                   takes 3 to 4GB of memory (default 4)
  --threads 4      how many threads each worker uses (default 4)
  --timeout 1800   give up on a PDF after this many seconds (default 1800)
  --retry-failed   try again PDFs that failed or timed out
  --force          extract every PDF again
  --max-pages 200  leave out PDFs with more pages than this. They aren't
                   logged, so a later run without it extracts them. At
                   about 4.7 seconds a page for each worker, a PDF of more
                   than about 380 pages times out after the default 30
                   minutes.

The defaults were the quickest on a 32-thread Ryzen 9 7950X with 61GB of
memory, timed on the same 306 pages of decision PDFs:

  workers x threads   seconds a page   peak memory
  1 x 8               1.78              6.3GB
  2 x 8               1.32             11.1GB
  4 x 4               1.18             16.4GB
  8 x 2               1.25             24.3GB

16 x 1 would have needed about 48GB, more than was free.

PDFs with the fewest pages are converted first, so the longest run last.
In a terminal it shows a progress bar of pages. Otherwise, for example
when its output goes to a file, it prints a line for each PDF. The
workers' own output, such as Docling's warnings, is thrown away: each
file's errors are in the log.

Only one run at a time should extract a council's PDFs: each run deletes
the temporary files it finds, which another run may be writing.


1. Make chunks
--------------

  python manage.py embeddings chunk --council EDE
  python manage.py embeddings chunk --council EDE,ADU
  python manage.py embeddings chunk --all-councils
  python manage.py embeddings chunk --type decisions,documents --since 2026-01-01 --council MAN

Writes data/<COUNCIL>/Embeddings/<type>.jsonl, one chunk per line, for
each type of data the council has.

  --type minutes,decisions   only these types, from minutes, decisions and
                             documents (default: all of them). The files
                             of other types are left alone.
  --since 2026-01-01         only meetings and decisions on or after this
                             date, and the documents linked from those
                             decisions
  --until 2026-12-31         only those on or before this date

Each run replaces the files of the types it's run for. Running again on
unchanged data writes identical files.

Each chunk has a header, which is embedded together with the text so that
a chunk can be found by council, committee, decision maker or date.

minutes

- Only councils whose minutes come from ModernGov have a chunker. Others
  are listed as skipped.
- Each agenda item's minutes become one or more chunks of up to 350 words.
  A long item is split between paragraphs, and a RESOLVED block is kept
  together.
- The header names the council, committee, date and minute:

    East Devon District Council · Cabinet · 16 September 2026
    Minute 36: LGR in Devon: Government Decision and Implications for EDDC

- Items under 15 words, such as "There were none.", are written with
  "embed": false and step 2 skips them. Declarations of interest are
  embedded whatever their length.

decisions

- Every decisions scraper writes the same JSON, so every council's
  decisions are chunked the same way.
- Each decision becomes one or more chunks of up to 350 words of its
  purpose followed by its text.
- The header names the council, decision maker, date and title:

    Manchester City Council · Strategic Director (Corporate Services) & City Treasurer · 6 August 2026
    Decision: P-1475 Domestic Abuse - Intersecting Needs Safe Accommodation (2026/06/09A)

  The date of the meeting some decision makers start with, as in
  "12/01/2026 - Cabinet", is left out.
- Decisions with procedural titles (apologies, declarations of interest,
  the minutes and urgent business) aren't embedded.
- A decision with no purpose or text is embedded as its header alone.
- The metadata lists the content_hash of each document linked from the
  decision.

documents

- The documents linked from decisions in range that step 0 extracted.
  Those it didn't, or that failed, are counted by status in the summary.
- Each document is split by docling-core's HybridChunker into chunks of up
  to 350 words. It splits at the section headings Docling found, then by
  size, and merges small neighbouring pieces in the same section.
- Each row of a table is a line. A table with no header row is written as
  labels and values ("Decision maker: City Treasurer"), and a table with
  one as "row label, column label = value".
- The header names the earliest decision in range linking to the
  document, then the document and the section:

    Manchester City Council · Strategic Director (Corporate Services) & City Treasurer · 6 August 2026
    Decision: P-1475 Domestic Abuse - Intersecting Needs Safe Accommodation (2026/06/09A)
    D3 P-1475 Domestic Abuse - Intersecting Needs Safe Accommodation · Notice of Decision Made

- Chunks under 15 words aren't embedded.
- The metadata has the pages and headings the chunk comes from, and every
  decision in range linking to the document.


2. Make embeddings
------------------

  AWS_PROFILE=dev-lgsf-dc python manage.py embeddings embed --model titan-v2 --council EDE
  python manage.py embeddings embed --model bge-m3 --all-councils

--model is one of:

  titan-v2  Amazon Titan Text Embeddings V2 (amazon.titan-embed-text-v2:0),
            on Bedrock in eu-west-2. Needs AWS credentials allowed to call
            bedrock:InvokeModel. If the SSO session has expired, run
            `aws sso login --profile dev-lgsf-dc`. EDE (946 chunks) takes
            about 20 seconds.

  bge-m3    BAAI/bge-m3, run locally on the CPU. The first run downloads
            about 4.5GB to ~/.cache/huggingface: two copies of the
            weights, pytorch_model.bin from the main branch and
            model.safetensors from a conversion pull request on the Hub.
            EDE takes about 4 minutes on a 16-core CPU, and all councils
            about 50 minutes.

Both models give vectors of 1024 dimensions, normalised to unit length, so
compare them by cosine distance.

Writes data/<COUNCIL>/Embeddings/<model>/minutes.jsonl, with one line per
chunk that has "embed": true:

  {"chunk_id": "EDE:minutes:2765:18127:0", "embedding": [...],
   "model": "amazon.titan-embed-text-v2:0", "text_hash": "...",
   "token_count": 412}

text_hash is the chunk's hash when it was embedded. token_count is how
many tokens the model read.

Reruns only embed what has changed:

- A chunk embedded before by the same model, whose text_hash hasn't
  changed, keeps its embedding. Only new or changed chunks are sent to the
  model, and embeddings of chunks that have gone are dropped.
- Each council's file is written as soon as that council is done. If a run
  stops partway through, for example because the SSO session expired, run
  the same command again: the councils already done are reused.
- After making chunks again (step 1), run step 2 again to bring the
  embeddings up to date.

At the end it prints a table per council of chunks embedded and reused,
and the tokens read.


3. Load into Postgres
---------------------

Start Postgres with pgvector, in Docker:

  docker compose -f lgsf/embeddings/db/compose.yaml up -d

It listens on localhost:5433, so it doesn't clash with a system Postgres
on 5432. The user is postgres, the password is postgres, and the database
is embeddings.

Load the chunks and one or more models' embeddings:

  uv run lgsf/embeddings/db/load.py --data-dir data --model titan-v2 --model bge-m3

load.py declares its own dependencies, so `uv run` installs them
separately. It doesn't import from lgsf, so it can be copied to another
project. To use a different database, set DATABASE_URL.

It creates these tables:

  chunk                one row per chunk, from every council's chunk file
  embedding_titan_v2   one table per model: chunk_id, model, text_hash,
  embedding_bge_m3     token_count and the vector

Each run drops and reloads `chunk` and the tables of the models passed
with --model, in one transaction. Tables of other models are left alone.
If anything fails, nothing is changed.

For each model it prints:

  missing   chunks with "embed": true that have no embedding. Run step 2
            for those councils.
  stale     embeddings of text the chunk no longer has.
  orphaned  embeddings of chunks that no longer exist.

Stale and orphaned embeddings mean step 1 was run again after step 2.
Run step 2 again for those councils, then load again.

To stop the database:

  docker compose -f lgsf/embeddings/db/compose.yaml down

The data is kept in a Docker volume. Add -v to delete it as well.


Searching
---------

  AWS_PROFILE=dev-lgsf-dc uv run python manage.py embeddings search --model titan-v2 "council tax rise"
  uv run python manage.py embeddings search --model bge-m3 --council EDE --since 2026-01-01

The question is embedded with the model given by --model, and compared
with that model's table. Results are closest first, by cosine distance:
0 is the same direction, and larger is further away. Each result shows:

- the chunk's header
- the start of its text
- a link to its page: the meeting, the decision or the document
- for a document, a link to the decision that links to it
- its chunk_id

By default each agenda item, decision or document appears at most once,
as its closest chunk.

Leave out the question to be asked for one question after another, until
a blank line. This is quicker with bge-m3, which takes several seconds to
load and is then loaded once for all the questions.

Options:

  --council EDE,ADU      only these councils
  --type decisions       only these types of chunk (comma-separated)
  --since 2026-01-01     only chunks dated on or after this date
  --until 2026-03-31     only chunks dated on or before this date
  --limit 20             how many results (default 10)
  --every-chunk          show every matching chunk, not one per source
  --full                 show each result's whole text

It needs psycopg, from the embeddings dependency group, and connects to
the database in lgsf/embeddings/db/compose.yaml, or to DATABASE_URL.

Search compares meaning, not words, so it's weak at exact strings such as
planning references and people's names.


Checking the vectors in SQL
---------------------------

Connect with:

  psql postgresql://postgres:postgres@localhost:5433/embeddings

The nearest neighbours of a chunk don't need a question embedding. For
example, for EDE's Cabinet minute on LGR, 16 September 2026:

  SELECT c.council, c.date, split_part(c.context, E'\n', 2) AS minute,
         e.embedding <=> (SELECT embedding FROM embedding_titan_v2
                          WHERE chunk_id = 'EDE:minutes:2765:18127:0')
             AS distance
  FROM embedding_titan_v2 e
  JOIN chunk c USING (chunk_id)
  ORDER BY distance
  LIMIT 10;

The first row is the chunk itself, at a distance of 0. Change both table
names to embedding_bge_m3 to compare models.


Adding a model
--------------

In embedders.py:

1. Subclass EmbedderBase and set:
   - name: used for --model, the output directory and the table name
   - model: the provider's model ID
   - dimensions
2. Implement embed_documents() and embed_query(). If the model embeds
   queries differently from documents, for example with a prefix or an
   instruction, apply it in embed_query() only.
3. Add the class to EMBEDDERS.

Then run `manage.py embeddings embed --model <name>`,
`load.py --model <name>` and `manage.py embeddings search --model <name>`.


Files
-----

  models.py       Chunk, the record every chunker writes
  chunkers.py     ChunkerBase, ModGovMinutesChunker, DecisionChunker and
                  DocumentChunker
  text.py         html_to_text() and split_text()
  extract.py      converting PDFs with Docling, and the extraction log
                  (step 0)
  documents.py    splitting Docling's documents, with HybridChunker
  commands.py     manage.py embeddings: its extract (step 0), chunk
                  (step 1), embed (step 2) and search subcommands
  embedders.py    EmbedderBase, TitanV2Embedder, BgeM3Embedder, EMBEDDERS
  vectors.py      embedding files, and deciding which chunks need embedding
  search.py       searching the embeddings in Postgres
  db/             compose.yaml and load.py (step 3)
