"""
Moving the decision documents on S3 to content keys, against the fake S3
client the storage tests use.
"""

import csv
import hashlib
import io
import json
import zipfile

import pytest

from lgsf.storage.tests.test_s3_storage import FakeS3
from scripts.migrate_documents import Migration, main

REPORT = b"%PDF-1.7 a report"
APPENDIX = b"%PDF-1.7 an appendix"
ERROR_PAGE = b"<!DOCTYPE html><html><body>Restricted</body></html>"


def sha256(content):
    return hashlib.sha256(content).hexdigest()


def content_key(content, extension):
    return sha256(content) + extension


def put(fake, key, content, metadata=True):
    fake.objects[key] = {
        "Body": content,
        "ContentType": "application/pdf",
        "Metadata": {"sha256": sha256(content)} if metadata else {},
    }


def old_document(fake, key, content, **kwargs):
    put(fake, f"data/SWA/Decisions/documents/{key}", content, **kwargs)


def stored_document(key, content, url=None):
    """What a scrape before content keys recorded for a document."""
    return {
        "url": url or f"https://democracy.swansea.gov.uk/documents/{key}",
        "title": "A document",
        "storage_key": key,
        "storage_backend": "s3",
        "content_hash": f"sha256:{sha256(content)}",
        "content_length": len(content),
        "etag": '"abc"',
        "last_modified": "Tue, 05 Mar 2024 10:54:01 GMT",
        "content_type": "application/pdf",
    }


def decision(fake, name, *documents):
    put(
        fake,
        f"data/SWA/Decisions/json/{name}.json",
        json.dumps({"raw_title": name, "documents": list(documents)}).encode(),
    )


def index(fake, *documents):
    put(
        fake,
        "data/SWA/Decisions/_index.json",
        json.dumps(
            {
                "version": 1,
                "decisions": {},
                "documents": {d["url"]: dict(d) for d in documents},
                "windows": {},
            }
        ).encode(),
    )


def read_json(fake, key):
    return json.loads(fake.objects[f"data/SWA/Decisions/{key}"]["Body"])


def new_keys(fake):
    return sorted(
        k.removeprefix("data/SWA/documents/")
        for k in fake.objects
        if k.startswith("data/SWA/documents/")
    )


@pytest.fixture
def fake():
    return FakeS3()


@pytest.fixture
def migration(fake, tmp_path):
    def build(**kwargs):
        return Migration(
            fake,
            "bucket",
            "data",
            workers=2,
            mapping_dir=tmp_path / "documents-migration",
            out=lambda *a: None,
            **kwargs,
        )

    return build


@pytest.fixture
def swansea(fake):
    """Two decisions, each with a document, and an index naming both."""
    report = stored_document("2024-02-28-2199-s100029.pdf", REPORT)
    appendix = stored_document("2024-02-28-2200-s100030.pdf", APPENDIX)
    old_document(fake, report["storage_key"], REPORT)
    old_document(fake, appendix["storage_key"], APPENDIX)
    decision(fake, "2024-02-28-2199", report)
    decision(fake, "2024-02-28-2200", appendix)
    index(fake, report, appendix)
    return report, appendix


# ---- moving ----


def test_documents_are_copied_to_their_content_keys(fake, migration, swansea):
    report = migration().migrate("SWA")

    assert not report.failed
    assert new_keys(fake) == sorted(
        [content_key(REPORT, ".pdf"), content_key(APPENDIX, ".pdf")]
    )
    copied = fake.objects[f"data/SWA/documents/{content_key(REPORT, '.pdf')}"]
    assert copied["Body"] == REPORT
    assert copied["Metadata"] == {"sha256": sha256(REPORT)}
    assert copied["ContentType"] == "application/pdf"


def test_the_json_and_index_name_the_new_keys(fake, migration, swansea):
    migration().migrate("SWA")

    document = read_json(fake, "json/2024-02-28-2199.json")["documents"][0]
    assert document["storage_key"] == content_key(REPORT, ".pdf")
    assert document["content_hash"] == f"sha256:{sha256(REPORT)}"
    assert document["etag"] == '"abc"'
    entries = read_json(fake, "_index.json")["documents"]
    assert sorted(e["storage_key"] for e in entries.values()) == new_keys(fake)


def test_rewritten_json_is_formatted_as_the_scrapers_write_it(fake, migration, swansea):
    migration().migrate("SWA")

    body = fake.objects["data/SWA/Decisions/json/2024-02-28-2199.json"]["Body"]
    assert body.decode() == json.dumps(json.loads(body), indent=4, sort_keys=True)


def test_the_extension_comes_from_the_bytes(fake, migration):
    """mgConvert2PDF.aspx error pages were stored as .pdf."""
    page = stored_document("2024-02-28-2199-1.pdf", ERROR_PAGE)
    old_document(fake, page["storage_key"], ERROR_PAGE)
    decision(fake, "2024-02-28-2199", page)
    index(fake, page)

    migration().migrate("SWA")

    assert new_keys(fake) == [content_key(ERROR_PAGE, ".html")]
    copied = fake.objects[f"data/SWA/documents/{content_key(ERROR_PAGE, '.html')}"]
    assert copied["ContentType"].startswith("text/html")


def test_only_the_first_8_kib_of_most_files_is_read(fake, migration):
    big = b"%PDF-1.7\n" + b"x" * 100_000
    document = stored_document("2024-02-28-2199-s1.pdf", big)
    old_document(fake, document["storage_key"], big)
    decision(fake, "2024-02-28-2199", document)
    index(fake, document)

    report = migration().migrate("SWA")

    assert report.ranged_reads == 1 and report.whole_reads == 0
    assert (
        fake.ops("get").count("data/SWA/Decisions/documents/2024-02-28-2199-s1.pdf")
        == 0
    )
    assert new_keys(fake) == [content_key(big, ".pdf")]


def test_a_zip_is_read_whole(fake, migration):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("padding.bin", b"\x01" * 20_000)
        archive.writestr(
            "[Content_Types].xml",
            '<Types><Override PartName="/word/document.xml" ContentType="'
            "application/vnd.openxmlformats-officedocument.wordprocessingml"
            '.document.main+xml"/></Types>',
        )
    docx = buffer.getvalue()
    document = stored_document("2024-02-28-2199-s1.docx", docx)
    old_document(fake, document["storage_key"], docx)
    decision(fake, "2024-02-28-2199", document)
    index(fake, document)

    report = migration().migrate("SWA")

    # [Content_Types].xml is past the first 8 KiB, so only the whole file
    # says it's a Word document.
    assert report.whole_reads == 1
    assert new_keys(fake) == [content_key(docx, ".docx")]


def test_an_object_with_no_sha256_metadata_is_read_and_hashed(fake, migration):
    big = b"%PDF-1.7\n" + b"y" * 20_000
    document = stored_document("2024-02-28-2199-s1.pdf", big)
    old_document(fake, document["storage_key"], big, metadata=False)
    decision(fake, "2024-02-28-2199", document)
    index(fake, document)

    report = migration().migrate("SWA")

    assert report.whole_reads == 1
    assert new_keys(fake) == [content_key(big, ".pdf")]
    copied = fake.objects[f"data/SWA/documents/{content_key(big, '.pdf')}"]
    assert copied["Metadata"] == {"sha256": sha256(big)}


def test_two_old_files_with_the_same_bytes_become_one_new_key(fake, migration):
    first = stored_document("2024-02-28-2199-s1.pdf", REPORT, url="https://a/1")
    second = stored_document("2024-03-01-2300-s2.pdf", REPORT, url="https://a/2")
    old_document(fake, first["storage_key"], REPORT)
    old_document(fake, second["storage_key"], REPORT)
    decision(fake, "2024-02-28-2199", first)
    decision(fake, "2024-03-01-2300", second)
    index(fake, first, second)

    report = migration().migrate("SWA")

    assert fake.ops("copy") == [f"data/SWA/documents/{content_key(REPORT, '.pdf')}"]
    assert report.to_copy == 1
    for name in ("2024-02-28-2199", "2024-03-01-2300"):
        document = read_json(fake, f"json/{name}.json")["documents"][0]
        assert document["storage_key"] == content_key(REPORT, ".pdf")


def test_an_empty_object_is_not_read_and_its_documents_become_unavailable(
    fake, migration, swansea
):
    empty = stored_document("2024-02-28-2199-s3.pdf", b"", url="https://a/empty")
    old_document(fake, empty["storage_key"], b"")
    report_document, appendix = swansea
    decision(fake, "2024-02-28-2199", report_document, empty)
    index(fake, report_document, appendix, empty)

    report = migration().migrate("SWA")

    assert report.empty == 1
    assert "data/SWA/Decisions/documents/2024-02-28-2199-s3.pdf" not in (
        fake.ops("range") + fake.ops("get")
    )
    documents = read_json(fake, "json/2024-02-28-2199.json")["documents"]
    assert documents[1] == {
        "url": "https://a/empty",
        "title": "A document",
        "unavailable": "empty",
    }
    assert "https://a/empty" not in read_json(fake, "_index.json")["documents"]
    assert not report.failed


def test_a_content_hash_that_disagrees_with_its_file_is_corrected(fake, migration):
    document = stored_document("2024-02-28-2199-s1.pdf", REPORT)
    document["content_hash"] = "sha256:" + "0" * 64
    old_document(fake, document["storage_key"], REPORT)
    decision(fake, "2024-02-28-2199", document)
    index(fake, document)

    report = migration().migrate("SWA")

    assert report.hash_disagreements == 1
    entry = read_json(fake, "_index.json")["documents"][document["url"]]
    assert entry["content_hash"] == f"sha256:{sha256(REPORT)}"
    rewritten = read_json(fake, "json/2024-02-28-2199.json")["documents"][0]
    assert rewritten["content_hash"] == f"sha256:{sha256(REPORT)}"


def test_the_mapping_is_written_before_anything_changes(
    fake, migration, swansea, tmp_path
):
    fake.fail_on = "data/SWA/documents/"

    migration().migrate("SWA")

    with (tmp_path / "documents-migration" / "SWA.csv").open() as f:
        rows = {row["old_key"]: row for row in csv.DictReader(f)}
    assert rows["2024-02-28-2199-s100029.pdf"]["new_key"] == content_key(REPORT, ".pdf")
    assert rows["2024-02-28-2199-s100029.pdf"]["sha256"] == sha256(REPORT)
    assert rows["2024-02-28-2199-s100029.pdf"]["size"] == str(len(REPORT))


def test_a_failed_copy_leaves_its_documents_naming_the_old_file(
    fake, migration, swansea
):
    fake.fail_on = content_key(REPORT, ".pdf")

    report = migration().migrate("SWA")

    assert report.errors
    document = read_json(fake, "json/2024-02-28-2199.json")["documents"][0]
    assert document["storage_key"] == "2024-02-28-2199-s100029.pdf"
    other = read_json(fake, "json/2024-02-28-2200.json")["documents"][0]
    assert other["storage_key"] == content_key(APPENDIX, ".pdf")
    assert not report.problems


def test_rerunning_after_an_interruption_finishes_without_repeating_work(
    fake, migration, swansea
):
    fake.fail_on = content_key(REPORT, ".pdf")
    migration().migrate("SWA")
    fake.fail_on = None
    fake.calls.clear()

    report = migration().migrate("SWA")

    assert not report.failed
    assert report.reused == 2
    reads = fake.ops("range") + fake.ops("get")
    assert [k for k in reads if "/Decisions/documents/" in k] == []
    assert fake.ops("copy") == [f"data/SWA/documents/{content_key(REPORT, '.pdf')}"]
    document = read_json(fake, "json/2024-02-28-2199.json")["documents"][0]
    assert document["storage_key"] == content_key(REPORT, ".pdf")


def test_a_run_over_a_migrated_council_changes_nothing(fake, migration, swansea):
    migration().migrate("SWA")
    fake.calls.clear()

    report = migration().migrate("SWA")

    assert fake.ops("put") == [] and fake.ops("copy") == []
    assert report.json_rewritten == 0 and not report.index_rewritten
    assert not report.failed


def test_a_council_scraped_after_the_change_but_before_migrating(fake, migration):
    """
    Some of its JSON already names content keys, which are left alone, and
    an old file whose bytes are already under their new key isn't copied.
    """
    old = stored_document("2024-02-28-2199-s1.pdf", REPORT, url="https://a/1")
    old_document(fake, old["storage_key"], REPORT)
    already = stored_document(content_key(REPORT, ".pdf"), REPORT, url="https://a/2")
    newer = stored_document(content_key(APPENDIX, ".pdf"), APPENDIX, url="https://a/3")
    put(fake, f"data/SWA/documents/{content_key(REPORT, '.pdf')}", REPORT)
    put(fake, f"data/SWA/documents/{content_key(APPENDIX, '.pdf')}", APPENDIX)
    decision(fake, "2024-02-28-2199", old)
    decision(fake, "2026-10-19-3000", already, newer)
    index(fake, old, already, newer)
    fake.calls.clear()

    report = migration().migrate("SWA")

    assert fake.ops("copy") == []
    assert report.already_there == 1
    assert fake.ops("put") == [
        "data/SWA/Decisions/json/2024-02-28-2199.json",
        "data/SWA/Decisions/_index.json",
    ]
    assert not report.failed


def test_json_naming_a_file_that_isnt_there_is_reported(fake, migration, swansea):
    lost = stored_document("2024-02-28-2199-s9.pdf", APPENDIX, url="https://a/lost")
    decision(fake, "2024-02-28-2201", lost)

    report = migration().migrate("SWA")

    assert (
        "json/2024-02-28-2201.json",
        "2024-02-28-2199-s9.pdf",
        "not there",
    ) in report.problems
    assert report.failed


# ---- a dry run ----


def test_a_dry_run_changes_nothing_on_s3(fake, migration, swansea):
    before = {k: dict(v) for k, v in fake.objects.items()}

    report = migration(dry_run=True).migrate("SWA")

    assert fake.objects == before
    assert fake.ops("put") == [] and fake.ops("copy") == []
    assert report.to_copy == 2
    assert report.copy_bytes == len(REPORT) + len(APPENDIX)
    assert report.json_rewritten == 2 and report.index_rewritten
    assert not report.problems


def test_a_real_run_reuses_what_a_dry_run_read(fake, migration, swansea):
    migration(dry_run=True).migrate("SWA")
    fake.calls.clear()

    migration().migrate("SWA")

    assert fake.ops("range") == []
    assert len(fake.ops("copy")) == 2


# ---- deleting the old files ----


def old_keys(fake):
    return sorted(
        k.removeprefix("data/SWA/Decisions/documents/")
        for k in fake.objects
        if k.startswith("data/SWA/Decisions/documents/")
    )


def test_delete_old_removes_files_that_have_moved(fake, migration, swansea):
    migration().migrate("SWA")

    report = migration().delete_old("SWA")

    assert old_keys(fake) == []
    assert report.deleted == 2
    assert len(new_keys(fake)) == 2


def test_delete_old_keeps_a_file_json_still_names(fake, migration, swansea):
    fake.fail_on = content_key(REPORT, ".pdf")
    migration().migrate("SWA")
    fake.fail_on = None

    report = migration().delete_old("SWA")

    assert old_keys(fake) == ["2024-02-28-2199-s100029.pdf"]
    assert report.kept == {"still named": 1}


def test_delete_old_keeps_a_file_it_has_no_mapping_for(fake, migration, swansea):
    migration().migrate("SWA")
    old_document(fake, "2024-03-01-2300-s5.pdf", APPENDIX)

    report = migration().delete_old("SWA")

    assert old_keys(fake) == ["2024-03-01-2300-s5.pdf"]
    assert report.kept == {"not in the mapping": 1}


def test_delete_old_removes_an_empty_file_once_nothing_names_it(fake, migration):
    empty = stored_document("2024-02-28-2199-s3.pdf", b"", url="https://a/empty")
    old_document(fake, empty["storage_key"], b"")
    decision(fake, "2024-02-28-2199", empty)
    index(fake, empty)
    migration().migrate("SWA")

    migration().delete_old("SWA")

    assert old_keys(fake) == []


def test_a_dry_run_of_delete_old_deletes_nothing(fake, migration, swansea):
    migration().migrate("SWA")

    report = migration(dry_run=True).delete_old("SWA")

    assert len(old_keys(fake)) == 2
    assert report.deletable == 2 and report.deleted == 0


# ---- which councils ----


def test_every_council_with_a_decisions_index_is_migrated(fake, migration):
    put(fake, "data/SWA/Decisions/_index.json", b"{}")
    put(fake, "data/MAN/Decisions/_index.json", b"{}")
    put(fake, "data/TST/Decisions/json/a.json", b"{}")
    put(fake, "data/KIR/Councillors/json/a.json", b"{}")

    assert migration().councils() == ["MAN", "SWA"]


def test_the_command_line(fake, swansea, monkeypatch, tmp_path, capsys):
    from lgsf.storage import s3

    monkeypatch.setattr(s3, "get_client", lambda: fake)
    monkeypatch.setenv("LGSF_S3_BUCKET", "bucket")
    monkeypatch.setenv("LGSF_S3_PREFIX", "data")
    monkeypatch.chdir(tmp_path)

    assert main(["--council", "SWA", "--dry-run"]) == 0
    assert fake.ops("copy") == []
    assert main(["--council", "SWA"]) == 0
    assert len(new_keys(fake)) == 2
    assert (tmp_path / "documents-migration" / "SWA.csv").exists()
    out = capsys.readouterr().out
    assert "SWA: 2 old files" in out


# ---- --to-prefix: migrate into a copy, leaving the original as it was ----


def snapshot(fake, prefix):
    return {k: dict(v) for k, v in fake.objects.items() if k.startswith(f"{prefix}/")}


@pytest.fixture
def swansea_with_more(fake, swansea):
    """Swansea's decisions plus files of other types the copy must carry."""
    put(fake, "data/SWA/Minutes/json/2026-01-01-council.json", b'{"title": "Council"}')
    put(fake, "data/SWA/Minutes/_index.json", b"{}")
    put(
        fake,
        f"data/SWA/documents/{content_key(b'%PDF already new', '.pdf')}",
        b"%PDF already new",
    )
    put(fake, "data/SWA/Decisions/runlog.json", b"{}")
    return swansea


def to_merged(migration):
    return migration(to_prefix="merged-data")


def test_the_original_prefix_is_left_exactly_as_it_was(
    fake, migration, swansea_with_more
):
    before = snapshot(fake, "data")

    report = to_merged(migration).migrate("SWA")

    assert not report.failed, report.lines()
    assert snapshot(fake, "data") == before
    assert not [
        op
        for op in fake.calls
        if op[0] in ("put", "copy", "delete") and op[1].startswith("data/")
    ]


def test_documents_go_to_content_keys_under_the_new_prefix(
    fake, migration, swansea_with_more
):
    to_merged(migration).migrate("SWA")

    copied = fake.objects[f"merged-data/SWA/documents/{content_key(REPORT, '.pdf')}"]
    assert copied["Body"] == REPORT
    assert copied["Metadata"] == {"sha256": sha256(REPORT)}
    assert not [
        k for k in fake.objects if k.startswith("merged-data/SWA/Decisions/documents/")
    ]


def test_the_copied_json_and_index_name_the_new_keys(
    fake, migration, swansea_with_more
):
    to_merged(migration).migrate("SWA")

    decision_json = json.loads(
        fake.objects["merged-data/SWA/Decisions/json/2024-02-28-2199.json"]["Body"]
    )
    assert decision_json["documents"][0]["storage_key"] == content_key(REPORT, ".pdf")
    index_json = json.loads(
        fake.objects["merged-data/SWA/Decisions/_index.json"]["Body"]
    )
    assert {d["storage_key"] for d in index_json["documents"].values()} == {
        content_key(REPORT, ".pdf"),
        content_key(APPENDIX, ".pdf"),
    }
    # and the originals still name the old keys
    assert (
        read_json(fake, "json/2024-02-28-2199.json")["documents"][0]["storage_key"]
        == "2024-02-28-2199-s100029.pdf"
    )


def test_everything_else_is_copied_unchanged(fake, migration, swansea_with_more):
    to_merged(migration).migrate("SWA")

    for key in (
        "SWA/Minutes/json/2026-01-01-council.json",
        "SWA/Minutes/_index.json",
        "SWA/Decisions/runlog.json",
        f"SWA/documents/{content_key(b'%PDF already new', '.pdf')}",
    ):
        assert (
            fake.objects[f"merged-data/{key}"]["Body"]
            == fake.objects[f"data/{key}"]["Body"]
        ), key


def test_a_decision_naming_no_documents_is_copied_not_rewritten(
    fake, migration, swansea_with_more
):
    decision(fake, "2024-03-01-0001")

    to_merged(migration).migrate("SWA")

    key = "SWA/Decisions/json/2024-03-01-0001.json"
    assert (
        fake.objects[f"merged-data/{key}"]["Body"]
        == fake.objects[f"data/{key}"]["Body"]
    )


def test_a_rerun_copies_only_what_changed_since(fake, migration, swansea_with_more):
    to_merged(migration).migrate("SWA")
    put(fake, "data/SWA/Minutes/json/2026-02-01-cabinet.json", b'{"title": "Cabinet"}')
    fake.calls.clear()

    report = to_merged(migration).migrate("SWA")

    assert report.rest_to_copy == 1
    assert report.to_copy == 0
    rest_copies = [k for op, k in fake.calls if op == "copy" and "/Minutes/" in k]
    assert rest_copies == ["merged-data/SWA/Minutes/json/2026-02-01-cabinet.json"]


def test_a_dry_run_writes_nothing_anywhere(fake, migration, swansea_with_more):
    before = dict(fake.objects)

    report = migration(to_prefix="merged-data", dry_run=True).migrate("SWA")

    assert fake.objects == before
    assert report.to_copy == 2
    assert report.rest_to_copy == 4
    assert any("other files unchanged" in line for line in report.lines())


def test_a_document_left_on_an_old_key_is_a_problem_in_the_copy(
    fake, migration, swansea_with_more
):
    """It can't be named from merged-data/: old-layout files aren't copied there."""
    fake.fail_on = content_key(APPENDIX, ".pdf")

    report = to_merged(migration).migrate("SWA")

    assert report.failed
    assert any("old-layout file" in why for _, _, why in report.problems)


def test_every_council_is_included_when_copying(fake, migration):
    put(fake, "data/SWA/Decisions/_index.json", b"{}")
    put(fake, "data/KIR/Councillors/json/a.json", b"{}")

    assert to_merged(migration).councils() == ["KIR", "SWA"]


def test_a_council_with_no_decisions_is_copied_whole(fake, migration):
    put(fake, "data/KIR/Councillors/json/a.json", b'{"name": "A"}')

    report = to_merged(migration).migrate("KIR")

    assert not report.failed
    assert (
        fake.objects["merged-data/KIR/Councillors/json/a.json"]["Body"]
        == b'{"name": "A"}'
    )


def test_the_same_prefix_is_not_a_copy(migration):
    assert migration(to_prefix="data/").to_prefix is None


def test_delete_old_is_refused_with_to_prefix(fake, swansea, monkeypatch, tmp_path):
    from lgsf.storage import s3

    monkeypatch.setattr(s3, "get_client", lambda: fake)
    monkeypatch.setenv("LGSF_S3_BUCKET", "bucket")
    monkeypatch.setenv("LGSF_S3_PREFIX", "data")
    with pytest.raises(SystemExit):
        main(["--to-prefix", "merged-data", "--delete-old"])


def test_the_command_line_with_to_prefix(fake, swansea, monkeypatch, tmp_path, capsys):
    from lgsf.storage import s3

    monkeypatch.setattr(s3, "get_client", lambda: fake)
    monkeypatch.setenv("LGSF_S3_BUCKET", "bucket")
    monkeypatch.setenv("LGSF_S3_PREFIX", "data")
    monkeypatch.chdir(tmp_path)

    assert main(["--council", "SWA", "--to-prefix", "merged-data"]) == 0
    assert "to s3://bucket/merged-data" in capsys.readouterr().out
    assert f"merged-data/SWA/documents/{content_key(REPORT, '.pdf')}" in fake.objects
