"""
Running the stages, and running them again.

Docling isn't loaded: extract.convert() is replaced with one that
returns the Manchester decision notice from fixtures/, whatever it's
given, and files are worked on in threads rather than worker processes.
The embedder is a fake. The files are made up in a temporary directory.

docling-core is needed to load the fixture document, so these tests are
skipped without the embeddings dependency group.
"""

import hashlib
import json
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

import pytest

from lgsf.embeddings import extract, pipeline
from lgsf.embeddings.embedders import FakeEmbedder
from lgsf.embeddings.extract import ConverterSettings, Device, ExtractStatus
from lgsf.embeddings.files import File
from lgsf.embeddings.sidecars import Outcome, Stage, read_status
from lgsf.embeddings.stores import LocalStore, S3Store
from lgsf.storage.documents.sniff import sniff_extension
from lgsf.storage.tests.test_s3_storage import FakeS3

FIXTURES = Path(__file__).parent / "fixtures"
MAN_DOCLING = FIXTURES / "MAN" / "decision-notice.docling.json"

PDF = b"%PDF-1.7\nA made-up PDF"
RESTRICTED = (FIXTURES / "formats" / "restricted-oldham.html").read_bytes()

ALL_STAGES = list(Stage)
RESULTS = ["chunks.jsonl", "docling-v2.json", "fake.jsonl", "md", "status.json"]


@pytest.fixture
def converted(monkeypatch):
    """The names of the streams extract.convert() is given."""
    docling_core = pytest.importorskip("docling_core.types.doc")
    document = docling_core.DoclingDocument.load_from_json(MAN_DOCLING)
    names = []

    def convert(stream):
        names.append(stream.name)
        return extract.Conversion(ExtractStatus.OK, 2, None, document)

    monkeypatch.setattr(extract, "convert", convert)
    monkeypatch.setattr(extract, "docling_version", lambda: "2.130.0")
    monkeypatch.setattr(extract, "converter_device", lambda: "cpu")
    # The made-up PDF has no metadata, and the real pdf_title() would
    # need pypdfium2 to say so.
    monkeypatch.setattr(extract, "pdf_title", lambda data: None)
    return names


@pytest.fixture
def store(tmp_path):
    return LocalStore(tmp_path)


def add_file(store, data, council="MAN") -> File:
    """A file stored under its content key, as the scrapers store it."""
    file = File(council, hashlib.sha256(data).hexdigest() + sniff_extension(data))
    store.write_bytes(file.document_key, data)
    return file


def run(
    store,
    files,
    stages=ALL_STAGES,
    embedder=None,
    threads=True,
    one_by_one=False,
    **options,
):
    """
    The outcomes of the files that needed something. ``one_by_one`` plans
    each file from its own status, as --file does, rather than from a
    listing.
    """
    embedder = embedder or FakeEmbedder()
    executor = ThreadPoolExecutor(max_workers=2) if threads else None
    pipeline_options = pipeline.Options(stages=stages, **options)
    plans = None
    if one_by_one:
        plans = [
            pipeline.plan(store, f, pipeline_options, [embedder.name]) for f in files
        ]
    outcomes = pipeline.run_files(
        store, files, pipeline_options, [embedder], executor, plans=plans
    )
    if executor:
        executor.shutdown()
    return [outcome for outcome in outcomes if not outcome.skipped]


def results(store, file):
    """The files written beside a document, by suffix."""
    return sorted(
        name.removeprefix(f"{file.document_key}.")
        for name in store.list_prefix(f"{file.document_key}.")
    )


def status(store, file):
    return read_status(store, file.document_key, file.content_hash)


def read_lines(store, key):
    return [json.loads(line) for line in store.read_bytes(key).splitlines()]


# A first run


def test_a_pdf_gets_every_result_file(store, converted):
    file = add_file(store, PDF)
    embedder = FakeEmbedder()
    [outcome] = run(store, [file], embedder=embedder)
    assert outcome.error is None
    assert results(store, file) == RESULTS
    assert converted == [file.storage_key]

    result = status(store, file)
    assert result.outcome == Outcome.OK
    assert result.error is None
    assert result.extract.status == ExtractStatus.OK
    assert result.extract.pages == 2
    assert result.extract.tool_version == "2.130.0"
    assert result.extract.device == "cpu"
    assert result.chunk.chunks == 4
    assert result.chunk.embeddable == 3
    assert result.embed["fake"].embedded == 3
    assert result.embed["fake"].reused == 0
    assert result.embed["fake"].tokens > 0
    assert len(embedder.texts) == 3


def test_result_file_contents(store, converted):
    file = add_file(store, PDF)
    run(store, [file])
    key = file.document_key
    chunks = read_lines(store, f"{key}.chunks.jsonl")
    assert [c["chunk_index"] for c in chunks] == [0, 1, 2, 3]
    assert [c["chunk_id"] for c in chunks] == [
        f"MAN:documents:{file.digest}:{i}" for i in range(4)
    ]
    assert {c["content_hash"] for c in chunks} == {file.content_hash}
    assert {c["storage_key"] for c in chunks} == {file.storage_key}
    # No metadata title, so the first heading is the title.
    assert chunks[1]["header"] == "Decision taken\nNotice of Decision Made"
    records = read_lines(store, f"{key}.fake.jsonl")
    assert [r["chunk_id"] for r in records] == [
        c["chunk_id"] for c in chunks if c["embed"]
    ]
    assert [r["text_hash"] for r in records] == [
        c["text_hash"] for c in chunks if c["embed"]
    ]
    assert {r["dimensions"] for r in records} == {4}
    assert b"Notice of Decision Made" in store.read_bytes(f"{key}.md")
    assert json.loads(store.read_bytes(f"{key}.docling-v2.json"))["name"]


def test_the_pdf_title_heads_the_chunks(store, converted, monkeypatch):
    monkeypatch.setattr(
        extract,
        "pdf_title",
        lambda data: "Microsoft Word - Decision notice P-1475.docx",
    )
    file = add_file(store, PDF)
    run(store, [file])
    chunks = read_lines(store, f"{file.document_key}.chunks.jsonl")
    assert chunks[1]["header"] == "Decision notice P-1475\nNotice of Decision Made"


@pytest.mark.parametrize(
    ("data", "extract_status", "outcome"),
    [
        (RESTRICTED, ExtractStatus.RESTRICTED, Outcome.RESTRICTED),
        (
            b"<html><body>Logon</body></html>",
            ExtractStatus.NOT_DOCUMENT,
            Outcome.NOT_A_DOCUMENT,
        ),
        (b"", ExtractStatus.EMPTY, Outcome.EMPTY),
        (
            b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1\x00",
            ExtractStatus.UNSUPPORTED,
            Outcome.NOT_A_DOCUMENT,
        ),
    ],
)
def test_left_out_files_only_get_a_status(
    store, converted, data, extract_status, outcome
):
    file = add_file(store, data)
    run(store, [file])
    assert results(store, file) == ["status.json"]
    result = status(store, file)
    assert result.extract.status == extract_status
    assert result.outcome == outcome
    assert result.extract.tool_version is None
    assert converted == []


def test_text_is_converted_as_markdown(store, converted):
    data = (FIXTURES / "formats" / "email.txt").read_bytes()
    file = add_file(store, data)
    assert file.storage_key.endswith(".txt")
    run(store, [file], stages=[Stage.EXTRACT])
    assert converted == [f"{file.digest}.md"]
    assert status(store, file).format == "text"


def test_a_file_that_has_changed_fails(store, converted):
    file = add_file(store, PDF)
    store.write_bytes(file.document_key, b"%PDF-2")
    run(store, [file])
    result = status(store, file)
    assert result.outcome == Outcome.FAILED
    assert result.extract.status == ExtractStatus.FAILED
    assert "SHA-256" in result.error
    assert results(store, file) == ["status.json"]
    assert converted == []


def test_a_missing_file_is_not_recorded(store, converted):
    file = File("MAN", "b" * 64 + ".pdf")
    [outcome] = run(store, [file])
    assert outcome.result.missing
    assert results(store, file) == []


def test_long_pdfs_are_left_for_later(store, converted, monkeypatch):
    monkeypatch.setattr(extract, "count_pages", lambda data: 500)
    file = add_file(store, PDF)
    [outcome] = run(store, [file], max_pages=100)
    assert outcome.result.too_long == 500
    assert results(store, file) == []
    run(store, [file])
    assert status(store, file).outcome == Outcome.OK


def test_a_conversion_that_raises_is_failed(store, converted, monkeypatch):
    def convert(stream):
        raise RuntimeError("Docling broke")

    monkeypatch.setattr(extract, "convert", convert)
    file = add_file(store, PDF)
    run(store, [file])
    result = status(store, file)
    assert result.outcome == Outcome.FAILED
    assert result.extract.status == ExtractStatus.FAILED
    assert result.error == "RuntimeError: Docling broke"
    assert results(store, file) == ["status.json"]


def test_a_partial_conversion_is_ok_with_the_error_noted(store, converted, monkeypatch):
    document = extract.load_docling_json(MAN_DOCLING.read_bytes())

    def convert(stream):
        return extract.Conversion(
            ExtractStatus.PARTIAL, 2, "Page 2 timed out", document
        )

    monkeypatch.setattr(extract, "convert", convert)
    file = add_file(store, PDF)
    run(store, [file])
    result = status(store, file)
    assert result.outcome == Outcome.OK
    assert result.extract.status == ExtractStatus.PARTIAL
    assert result.error == "Page 2 timed out"
    assert results(store, file) == RESULTS


# Running again


def test_running_again_writes_nothing(store, converted, monkeypatch):
    files = [add_file(store, PDF), add_file(store, RESTRICTED), add_file(store, b"")]
    run(store, files)
    before = {key: store.read_bytes(key) for key in store.list_prefix("MAN/documents/")}

    writes = []
    monkeypatch.setattr(store, "write_bytes", lambda key, data: writes.append(key))
    embedder = FakeEmbedder()
    outcomes = pipeline.run_files(store, files, pipeline.Options(), [embedder])
    assert [o.skipped for o in outcomes] == [True, True, True]
    assert writes == []
    assert converted == [files[0].storage_key]
    assert embedder.texts == []
    after = {key: store.read_bytes(key) for key in store.list_prefix("MAN/documents/")}
    assert after == before


def test_a_deleted_status_is_done_again(store, converted):
    files = [add_file(store, PDF), add_file(store, b"%PDF-1.7\nAnother")]
    run(store, files)
    assert len(converted) == 2
    Path(store.path(f"{files[0].document_key}.status.json")).unlink()
    outcomes = run(store, files)
    assert [o.file for o in outcomes] == [files[0]]
    assert len(converted) == 3


def test_failures_are_retried_when_asked(store, converted):
    file = add_file(store, PDF)
    store.write_bytes(file.document_key, b"%PDF-2")
    run(store, [file])
    assert status(store, file).outcome == Outcome.FAILED
    store.write_bytes(file.document_key, PDF)
    assert run(store, [file]) == []
    run(store, [file], retry_failed=True)
    assert status(store, file).outcome == Outcome.OK


def test_force(store, converted):
    file = add_file(store, PDF)
    run(store, [file])
    embedder = FakeEmbedder()
    run(store, [file], embedder=embedder, force=True)
    assert len(converted) == 2
    assert len(embedder.texts) == 3


def test_a_new_chunker_version(store, converted, monkeypatch):
    file = add_file(store, PDF)
    run(store, [file])
    monkeypatch.setattr("lgsf.embeddings.records.CHUNKER_VERSION", 999)
    embedder = FakeEmbedder()
    [outcome] = run(store, [file], embedder=embedder)
    # Chunked and embedded again, but not converted again, and the chunks'
    # text is the same, so every embedding is reused.
    assert converted == [file.storage_key]
    assert outcome.result.chunk is not None
    assert embedder.texts == []
    result = status(store, file)
    assert result.embed["fake"].reused == 3
    assert result.chunk.version == 999
    assert (
        read_lines(store, f"{file.document_key}.chunks.jsonl")[0]["chunker_version"]
        == 999
    )


def test_a_changed_title_is_embedded_again(store, converted, monkeypatch):
    file = add_file(store, PDF)
    run(store, [file])
    monkeypatch.setattr("lgsf.embeddings.records.CHUNKER_VERSION", 2)
    monkeypatch.setattr(extract, "pdf_title", lambda data: "Retitled")
    embedder = FakeEmbedder()
    run(store, [file], embedder=embedder)
    assert converted == [file.storage_key]
    assert len(embedder.texts) == 3
    assert all(text.startswith("Retitled\n") for text in embedder.texts)


def test_stages_one_at_a_time(store, converted):
    file = add_file(store, PDF)
    run(store, [file], stages=[Stage.EXTRACT])
    assert results(store, file) == ["docling-v2.json", "md", "status.json"]
    assert status(store, file).chunk is None
    run(store, [file], stages=[Stage.CHUNK])
    assert results(store, file) == [
        "chunks.jsonl",
        "docling-v2.json",
        "md",
        "status.json",
    ]
    run(store, [file], stages=[Stage.EMBED])
    assert results(store, file) == RESULTS
    assert converted == [file.storage_key]


def test_chunking_without_embedding_leaves_the_embeddings_to_redo(
    store, converted, monkeypatch
):
    file = add_file(store, PDF)
    run(store, [file])
    monkeypatch.setattr("lgsf.embeddings.records.CHUNKER_VERSION", 999)
    run(store, [file], stages=[Stage.CHUNK])
    assert "fake.jsonl" not in status(store, file).result_suffixes()
    run(store, [file], stages=[Stage.EMBED])
    assert status(store, file).embedded("fake")


def test_another_embedder_adds_its_own_file(store, converted):
    file = add_file(store, PDF)
    run(store, [file])

    class Other(FakeEmbedder):
        name = "other"
        model = "other-1"

    [outcome] = run(store, [file], embedder=Other())
    assert outcome.embedded_with == ["other"]
    assert results(store, file) == sorted([*RESULTS, "other.jsonl"])
    assert sorted(status(store, file).embed) == ["fake", "other"]


# Errors


class BrokenEmbedder(FakeEmbedder):
    def embed_documents(self, texts):
        raise RuntimeError("ThrottlingException")


def test_an_embedding_error_is_recorded_and_retried(store, converted):
    files = [add_file(store, PDF), add_file(store, RESTRICTED)]
    outcomes = run(store, files, embedder=BrokenEmbedder())
    # The run carried on.
    assert len(outcomes) == 2
    entry = status(store, files[0]).embed["fake"]
    assert entry.error == "RuntimeError: ThrottlingException"
    assert "fake.jsonl" not in results(store, files[0])
    assert status(store, files[0]).outcome == Outcome.OK

    embedder = FakeEmbedder()
    [outcome] = run(store, files, embedder=embedder)
    assert outcome.result.extract is None
    assert len(embedder.texts) == 3
    assert status(store, files[0]).embed["fake"].error is None


def test_a_chunking_error_is_recorded_and_retried(store, converted, monkeypatch):
    file = add_file(store, PDF)
    run(store, [file], stages=[Stage.EXTRACT])

    def broken(*args, **kwargs):
        raise RuntimeError("docling-core broke")

    monkeypatch.setattr(pipeline, "chunk_document", broken)
    run(store, [file])
    result = status(store, file)
    assert result.chunk.error == "RuntimeError: docling-core broke"
    assert result.chunk.inputs_hash is None
    assert result.embed == {}
    assert result.outcome == Outcome.OK
    assert "chunks.jsonl" not in results(store, file)

    monkeypatch.undo()
    run(store, [file])
    assert status(store, file).chunked
    assert converted == [file.storage_key]


# Missing inputs


def test_a_missing_docling_document_is_extracted_again(store, converted):
    file = add_file(store, PDF)
    run(store, [file])
    Path(store.path(f"{file.document_key}.docling-v2.json")).unlink()
    [outcome] = run(store, [file])
    assert outcome.error is None
    assert converted == [file.storage_key, file.storage_key]
    assert results(store, file) == RESULTS
    assert status(store, file).embedded("fake")


def test_an_unreadable_docling_document_is_extracted_again(store, converted):
    file = add_file(store, PDF)
    run(store, [file], stages=[Stage.EXTRACT])
    store.write_bytes(f"{file.document_key}.docling-v2.json", b"{")
    run(store, [file])
    assert converted == [file.storage_key, file.storage_key]
    assert status(store, file).chunked


def test_a_missing_docling_document_without_the_extract_stage(
    store, converted, monkeypatch
):
    file = add_file(store, PDF)
    run(store, [file])
    Path(store.path(f"{file.document_key}.docling-v2.json")).unlink()
    # plan_files() sees it's gone from the listing: nothing to chunk.
    assert run(store, [file], stages=[Stage.CHUNK, Stage.EMBED]) == []
    # Planned one file at a time, the chunk stage finds it gone.
    monkeypatch.setattr("lgsf.embeddings.records.CHUNKER_VERSION", 999)
    [outcome] = run(store, [file], stages=[Stage.CHUNK, Stage.EMBED], one_by_one=True)
    assert "docling-v2.json missing" in outcome.note
    result = status(store, file)
    assert result.extract is None
    assert result.outcome == Outcome.FAILED
    run(store, [file])
    assert status(store, file).embedded("fake")


def test_missing_chunks_are_chunked_again(store, converted):
    file = add_file(store, PDF)
    run(store, [file])
    Path(store.path(f"{file.document_key}.chunks.jsonl")).unlink()
    embedder = FakeEmbedder()
    [outcome] = run(store, [file], embedder=embedder)
    assert outcome.result.chunk is not None
    assert outcome.result.extract is None
    assert converted == [file.storage_key]
    assert embedder.texts == []  # the embeddings file is still good
    assert results(store, file) == RESULTS


def test_unreadable_chunks_are_chunked_again(store, converted):
    file = add_file(store, PDF)
    run(store, [file])
    store.write_bytes(f"{file.document_key}.chunks.jsonl", b"{")
    # A status that still vouches for the chunks, and needs embedding.
    stale = status(store, file)
    stale.embed = {}
    pipeline.write_status(store, file.document_key, stale)
    [outcome] = run(store, [file])
    assert "chunks.jsonl missing or unreadable" in outcome.note
    assert status(store, file).chunk is None
    run(store, [file])
    assert status(store, file).embedded("fake")


def test_stopping_while_embedding(store, converted):
    file = add_file(store, PDF)
    run(store, [file], stages=[Stage.EXTRACT, Stage.CHUNK])

    class StoppedEmbedder(FakeEmbedder):
        def embed_documents(self, texts):
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        pipeline.run_files(store, [file], pipeline.Options(), [StoppedEmbedder()])


# One file in this process


def test_run_file(store, converted):
    file = add_file(store, PDF)
    embedder = FakeEmbedder()
    outcome = pipeline.run_file(store, file, pipeline.Options(), [embedder])
    assert not outcome.skipped
    assert outcome.status.outcome == Outcome.OK
    assert results(store, file) == RESULTS
    assert len(embedder.texts) == 3

    again = pipeline.run_file(store, file, pipeline.Options(), [FakeEmbedder()])
    assert again.skipped
    assert again.status == outcome.status


def test_run_files_without_an_executor(store, converted):
    files = [add_file(store, PDF), add_file(store, RESTRICTED)]
    outcomes = run(store, files, threads=False)
    assert [o.status.outcome for o in outcomes] == [Outcome.OK, Outcome.RESTRICTED]


# Store errors


class FlakyStore(LocalStore):
    """Raises on the first write of a key matching ``fail_on``."""

    def __init__(self, root, fail_on):
        super().__init__(root)
        self.fail_on = fail_on
        self.failed = False

    def write_bytes(self, key, data):
        if not self.failed and self.fail_on in key:
            self.failed = True
            raise OSError("S3 is having a moment")
        super().write_bytes(key, data)


def test_a_store_error_while_extracting_records_nothing(tmp_path, converted):
    store = FlakyStore(tmp_path, fail_on=".docling-v2.json")
    file = add_file(store, PDF)
    [outcome] = run(store, [file])
    assert "S3 is having a moment" in outcome.error
    assert results(store, file) == []
    [outcome] = run(store, [file])
    assert outcome.error is None
    assert converted == [file.storage_key, file.storage_key]
    assert results(store, file) == RESULTS


def test_a_store_error_before_docling_is_reported_and_the_run_carries_on(
    store, converted, monkeypatch
):
    files = [add_file(store, PDF), add_file(store, b"%PDF-1.7\nAnother")]
    download_to = store.download_to

    def broken_download(key, directory):
        if key == files[0].document_key:
            raise RuntimeError("403 Forbidden")
        return download_to(key, directory)

    monkeypatch.setattr(store, "download_to", broken_download)
    outcomes = run(store, files, threads=False)
    assert "403 Forbidden" in outcomes[0].error
    assert results(store, files[0]) == []
    assert outcomes[1].status.outcome == Outcome.OK

    outcome = pipeline.run_file(store, files[0], pipeline.Options(), [FakeEmbedder()])
    assert "403 Forbidden" in outcome.error


# Write ordering


class StopAt(LocalStore):
    """Stops the run (as Ctrl-C would) at the nth write of a key."""

    def __init__(self, root, key_part, nth):
        super().__init__(root)
        self.key_part = key_part
        self.nth = nth
        self.count = 0

    def write_bytes(self, key, data):
        if self.key_part in key:
            self.count += 1
            if self.count == self.nth:
                raise KeyboardInterrupt
        super().write_bytes(key, data)


def test_stopping_between_the_chunks_and_the_status(tmp_path, converted, monkeypatch):
    store = StopAt(tmp_path, ".status.json", nth=3)  # done, superseded, final
    file = add_file(store, PDF)
    run(store, [file], threads=False)
    monkeypatch.setattr("lgsf.embeddings.records.CHUNKER_VERSION", 999)
    with pytest.raises(KeyboardInterrupt):
        run(store, [file], threads=False)
    # New chunks are on disk, and the status doesn't vouch for them.
    chunks = read_lines(store, f"{file.document_key}.chunks.jsonl")
    assert chunks[0]["chunker_version"] == 999
    result = status(store, file)
    assert result.extracted
    assert result.chunk is None
    assert result.embed == {}
    assert result.outcome == Outcome.FAILED
    assert result.error == "superseded: chunk running again"
    # The next run chunks and embeds, without converting.
    store.nth = 0
    run(store, [file], threads=False)
    assert converted == [file.storage_key]
    assert status(store, file).embedded("fake")


def test_stopping_between_a_forced_extraction_and_the_status(
    tmp_path, converted, monkeypatch
):
    store = StopAt(tmp_path, ".status.json", nth=3)
    file = add_file(store, PDF)
    run(store, [file], threads=False)
    with pytest.raises(KeyboardInterrupt):
        run(store, [file], threads=False, force=True)
    result = status(store, file)
    assert result.extract is None
    assert result.error == "superseded: extract running again"
    store.nth = 0
    run(store, [file], threads=False)
    assert status(store, file).embedded("fake")


def test_a_file_with_no_status_gets_no_superseded_status(store, converted):
    writes = []
    write_bytes = store.write_bytes
    store.write_bytes = lambda key, data: (writes.append(key), write_bytes(key, data))
    file = add_file(store, PDF)
    writes.clear()
    run(store, [file], threads=False)
    assert [w for w in writes if w.endswith(".status.json")] == [
        f"{file.document_key}.status.json"
    ]


# Embedding failures


def test_failed_chunks_are_sent_again_next_run(store, converted):
    from lgsf.embeddings.embedders import Embedding, Failed

    class HalfBroken(FakeEmbedder):
        def embed_documents(self, texts):
            self.texts.extend(texts)
            return [
                Failed("ThrottlingException")
                if i == 0
                else Embedding(self.vector(t), 3)
                for i, t in enumerate(texts)
            ]

    file = add_file(store, PDF)
    run(store, [file], embedder=HalfBroken())
    entry = status(store, file).embed["fake"]
    assert entry.embedded == 2
    assert entry.error == "1 of 3 chunks not embedded: ThrottlingException"
    assert len(read_lines(store, f"{file.document_key}.fake.jsonl")) == 2

    embedder = FakeEmbedder()
    run(store, [file], embedder=embedder)
    assert len(embedder.texts) == 1
    entry = status(store, file).embed["fake"]
    assert (entry.embedded, entry.reused, entry.error) == (1, 2, None)
    assert len(read_lines(store, f"{file.document_key}.fake.jsonl")) == 3


# Planning from a listing


def test_plan_files_reads_only_the_statuses_the_listing_has(converted):
    client = FakeS3()
    store = S3Store("bucket", "merged-data", client=client)
    files = [add_file(store, PDF), add_file(store, b"%PDF-1.7\nAnother")]
    run(store, files[:1], threads=False)
    client.calls.clear()
    plans = pipeline.plan_files(store, files, pipeline.Options(), ["fake"])
    assert [p.nothing_to_do for p in plans] == [True, False]
    assert [p.size for p in plans] == [len(PDF), len(b"%PDF-1.7\nAnother")]
    assert client.ops("list") == ["merged-data/MAN/documents/"]
    assert client.ops("get") == [f"merged-data/{files[0].document_key}.status.json"]


def test_plan_files_notices_missing_outputs(store, converted):
    files = [add_file(store, PDF), add_file(store, b"%PDF-1.7\nAnother")]
    run(store, files)
    Path(store.path(f"{files[0].document_key}.docling-v2.json")).unlink()
    Path(store.path(f"{files[1].document_key}.chunks.jsonl")).unlink()
    plans = pipeline.plan_files(store, files, pipeline.Options(), ["fake"])
    assert plans[0].extract
    assert plans[1].chunk and not plans[1].extract


# Worker processes


class RecordingExecutor:
    """Runs what it's given at once, in this thread, noting the order."""

    def __init__(self, raising=None):
        self.submitted = []
        self.raising = raising

    def submit(self, fn, *args):
        self.submitted.append(args[0])
        future = Future()
        if self.raising:
            future.set_exception(self.raising)
        else:
            future.set_result(fn(*args))
        return future

    def shutdown(self, **kwargs):
        pass


def test_larger_files_are_submitted_last(store, converted):
    files = [
        add_file(store, PDF + b" and a lot more of it"),
        add_file(store, PDF),
        add_file(store, PDF + b" more"),
    ]
    executor = RecordingExecutor()
    plans = pipeline.plan_files(store, files, pipeline.Options(), ["fake"])
    pipeline.run_files(
        store, files, pipeline.Options(), [FakeEmbedder()], executor, plans=plans
    )
    assert executor.submitted == [files[1], files[2], files[0]]


def test_a_dead_worker_pool_is_reported_once(store, converted):
    files = [add_file(store, PDF), add_file(store, b"%PDF-1.7\nAnother")]
    executor = RecordingExecutor(raising=BrokenProcessPool("gone"))
    outcomes = []
    with pytest.raises(BrokenProcessPool):
        pipeline.run_files(
            store,
            files,
            pipeline.Options(),
            [FakeEmbedder()],
            executor,
            outcomes.append,
        )
    assert len(outcomes) == 2
    assert all("BrokenProcessPool" in o.error for o in outcomes)
    assert results(store, files[0]) == []


# Converter settings


def test_the_converter_is_configured_from_the_options(store, converted, monkeypatch):
    configured = []
    monkeypatch.setattr(extract, "configure", configured.append)
    settings = ConverterSettings(threads=2, timeout=60, device=Device.CPU)
    pipeline.run_file(store, add_file(store, PDF), pipeline.Options(converter=settings))
    assert configured == [settings]


# Without Docling


@pytest.fixture
def no_docling(monkeypatch):
    for name in ("docling", "docling_core", "pypdfium2", "torch", "pandas"):
        monkeypatch.setitem(sys.modules, name, None)
        for module in list(sys.modules):
            if module.startswith(f"{name}."):
                monkeypatch.setitem(sys.modules, module, None)


def test_dispatch_without_docling(store, no_docling):
    assert extract.docling_version() is None
    assert extract.count_pages(b"%PDF-1.7") is None
    assert extract.pdf_title(b"%PDF-1.7") is None

    restricted = add_file(store, RESTRICTED)
    empty = add_file(store, b"")
    logon = add_file(store, b"<html><body>Logon</body></html>")
    pdf = add_file(store, PDF)
    changed = File("MAN", "c" * 64 + ".pdf")
    store.write_bytes(changed.document_key, b"%PDF-1.7 other bytes")
    missing = File("MAN", "d" * 64 + ".pdf")

    for file, outcome in (
        (restricted, Outcome.RESTRICTED),
        (empty, Outcome.EMPTY),
        (logon, Outcome.NOT_A_DOCUMENT),
        (changed, Outcome.FAILED),
    ):
        result = pipeline.dispatch(store, file)
        assert not result.needs_converter
        assert result.status.outcome == outcome
        assert results(store, file) == ["status.json"]
        assert pipeline.dispatch(store, file).skipped

    result = pipeline.dispatch(store, pdf)
    assert result.needs_converter
    assert result.result.format == "pdf"
    assert results(store, pdf) == []

    assert pipeline.dispatch(store, missing).result.missing


def test_dispatch_embeds_what_is_chunked(store, converted):
    file = add_file(store, PDF)
    run(store, [file], stages=[Stage.EXTRACT, Stage.CHUNK])
    embedder = FakeEmbedder()
    outcome = pipeline.dispatch(store, file, embedders=[embedder])
    assert outcome.embedded_with == ["fake"]
    assert len(embedder.texts) == 3
    assert not outcome.needs_converter
