"""
The JSON Schemas in schemas/v1 describe what the pipeline writes: the
fixtures and freshly written files are validated against them.
"""

import json
from pathlib import Path

import jsonschema
import pytest

from lgsf.embeddings.embedders import FakeEmbedder
from lgsf.embeddings.records import embed_chunks, read_chunks

from .test_sidecars import done_status, extracted

SCHEMAS = Path(__file__).parent.parent / "schemas" / "v1"
FIXTURES = Path(__file__).parent / "fixtures"


def validator(name):
    schema = json.loads((SCHEMAS / f"{name}.schema.json").read_text())
    jsonschema.Draft202012Validator.check_schema(schema)
    return jsonschema.Draft202012Validator(
        schema, format_checker=jsonschema.FormatChecker()
    )


def validate(name, instance):
    errors = sorted(validator(name).iter_errors(instance), key=str)
    assert not errors, "\n".join(error.message for error in errors)


@pytest.mark.parametrize("name", ["chunks", "embeddings", "status"])
def test_schemas_are_valid(name):
    validator(name)


def test_expected_chunks_fixture():
    data = (FIXTURES / "MAN" / "decision-notice.expected-chunks.jsonl").read_bytes()
    lines = [json.loads(line) for line in data.splitlines()]
    assert len(lines) == 4
    for line in lines:
        validate("chunks", line)


def test_embedding_records():
    data = (FIXTURES / "MAN" / "decision-notice.expected-chunks.jsonl").read_bytes()
    records = embed_chunks(read_chunks(data), FakeEmbedder(), {}).records
    assert records
    for record in records:
        validate("embeddings", json.loads(record.as_json()))


def test_status_of_a_file_through_every_stage():
    validate("status", json.loads(done_status().as_json()))


def test_status_of_a_file_only_extracted():
    validate("status", json.loads(extracted().as_json()))


def test_status_of_a_failed_file():
    from lgsf.embeddings.extract import ExtractStatus
    from lgsf.embeddings.sidecars import Outcome

    status = extracted(ExtractStatus.FAILED)
    status.outcome = Outcome.FAILED
    status.error = "RuntimeError: Docling broke"
    validate("status", json.loads(status.as_json()))


def test_additional_fields_are_allowed():
    data = json.loads(done_status().as_json())
    data["later"] = True
    data["stages"]["extract"]["later"] = True
    validate("status", data)


def test_a_wrong_outcome_is_refused():
    data = json.loads(done_status().as_json())
    data["outcome"] = "skipped"
    with pytest.raises(AssertionError):
        validate("status", data)


def test_freshly_written_files(tmp_path):
    """Everything a run writes beside a PDF, against the schemas."""
    pytest.importorskip("docling_core")
    from lgsf.embeddings import pipeline
    from lgsf.embeddings.stores import LocalStore

    from .test_pipeline import PDF, add_file

    store = LocalStore(tmp_path)
    file = add_file(store, PDF)
    monkeypatch = pytest.MonkeyPatch()
    with monkeypatch.context() as m:
        from lgsf.embeddings import extract

        document = extract.load_docling_json(
            (FIXTURES / "MAN" / "decision-notice.docling.json").read_bytes()
        )
        m.setattr(
            extract,
            "convert",
            lambda stream: extract.Conversion(
                extract.ExtractStatus.OK, 2, None, document
            ),
        )
        m.setattr(extract, "docling_version", lambda: "2.130.0")
        m.setattr(extract, "converter_device", lambda: "cpu")
        m.setattr(extract, "pdf_title", lambda data: None)
        pipeline.run_file(store, file, pipeline.Options(), [FakeEmbedder()])

    key = file.document_key
    validate("status", json.loads(store.read_bytes(f"{key}.status.json")))
    for line in store.read_bytes(f"{key}.chunks.jsonl").splitlines():
        validate("chunks", json.loads(line))
    for line in store.read_bytes(f"{key}.fake.jsonl").splitlines():
        validate("embeddings", json.loads(line))
