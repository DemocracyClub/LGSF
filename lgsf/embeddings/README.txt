Embeddings
==========

This package turns scraped council data into chunks of text and creates
embeddings (vectors) of them, which can then be loaded into Postgres with
pgvector and searched by similarity. For now it handles one source:
minutes from ModernGov's meeting XML.

There are three steps. Each reads the files the step before it wrote:

  1. python manage.py embeddings chunk
                                  chunks minutes into
                                  data/<COUNCIL>/Embeddings/minutes.jsonl
  2. python manage.py embeddings embed
                                  embeds those chunks into
                                  data/<COUNCIL>/Embeddings/<model>/
  3. uv run lgsf/embeddings/db/load.py
                                  loads chunks and embeddings into Postgres

Then `python manage.py embeddings search` searches them with a question.

Nothing is fetched from council websites. Step 1 reads the minutes already
in data/<COUNCIL>/Minutes/, so run the minutes scraper
(python manage.py minutes) or python manage.py sync first.

More detail, and the reasons for the design, are in
docs/embeddings-plan.md (chunking) and docs/embeddings-creation-plan.md
(embedding and loading).


Setup
-----

Step 2 needs packages that aren't installed by default:

  uv sync --group embeddings

This installs boto3, sentence-transformers and a CPU-only build of torch.
They're kept out of the default install, and out of the Lambda layer,
because torch is large. A plain `uv sync` removes them again, so run
`uv sync --group embeddings` after one.

The commands below are run from the repository root with the virtualenv
activated (source .venv/bin/activate).


1. Make chunks
--------------

  python manage.py embeddings chunk --council EDE
  python manage.py embeddings chunk --council EDE,ADU
  python manage.py embeddings chunk --all-councils

Writes data/<COUNCIL>/Embeddings/minutes.jsonl, one chunk per line.

- Only councils whose minutes come from ModernGov have a chunker. Others
  are listed as skipped.
- Each agenda item's minutes become one or more chunks of up to 350 words.
  A long item is split between paragraphs, and a RESOLVED block is kept
  together.
- Each chunk has a header naming the council, committee, date and minute,
  for example:

    East Devon District Council · Cabinet · 16 September 2026
    Minute 36: LGR in Devon: Government Decision and Implications for EDDC

  The header and text together are what gets embedded.
- Items under 15 words, such as "There were none.", are written with
  "embed": false and step 2 skips them. Declarations of interest are
  embedded whatever their length.
- Each run replaces the file. Running again on unchanged data writes an
  identical file.


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

- the chunk's header (council, committee, date, minute)
- the start of its text
- a link to the meeting
- its chunk_id

By default each agenda item appears at most once, as its closest chunk.

Leave out the question to be asked for one question after another, until
a blank line. This is quicker with bge-m3, which takes several seconds to
load and is then loaded once for all the questions.

Options:

  --council EDE,ADU      only these councils
  --since 2026-01-01     only meetings on or after this date
  --until 2026-03-31     only meetings on or before this date
  --limit 20             how many results (default 10)
  --every-chunk          show every matching chunk, not one per agenda item
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
  chunkers.py     ChunkerBase and ModGovMinutesChunker
  text.py         html_to_text() and split_text()
  commands.py     manage.py embeddings: its chunk (step 1), embed
                  (step 2) and search subcommands
  embedders.py    EmbedderBase, TitanV2Embedder, BgeM3Embedder, EMBEDDERS
  vectors.py      embedding files, and deciding which chunks need embedding
  search.py       searching the embeddings in Postgres
  db/             compose.yaml and load.py (step 3)
