"""
manage.py embeddings chunk: choosing which chunkers run with --type, and
the date range given with --since and --until.

The data directory is a temporary copy of the test fixtures. Council
names and minutes CMSs come from the councils' real metadata.json.
"""

import argparse
import io
import shutil
from pathlib import Path

import pytest

from lgsf.conf import settings
from lgsf.embeddings.commands import Command, chunk_types
from lgsf.embeddings.models import ChunkType

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    path = tmp_path / "data"
    shutil.copytree(FIXTURES, path)
    monkeypatch.setattr(settings, "DATA_DIR_NAME", str(path))
    return path


def run(*args):
    stdout = io.StringIO()
    Command(["embeddings", *args], stdout).execute()
    return stdout.getvalue()


def read_lines(path):
    return path.read_text(encoding="utf-8").splitlines()


def test_chunk_types():
    assert chunk_types("minutes") == [ChunkType.MINUTES]
    assert chunk_types(" minutes, ") == [ChunkType.MINUTES]
    with pytest.raises(argparse.ArgumentTypeError):
        chunk_types("agendas")


def test_chunk_runs_every_type_by_default(data_dir):
    output = run("chunk", "--council", "EDE")
    assert len(read_lines(data_dir / "EDE" / "Embeddings" / "minutes.jsonl")) > 1
    assert "1 meetings" in output


def test_chunk_since_and_until(data_dir):
    minutes_path = data_dir / "EDE" / "Embeddings" / "minutes.jsonl"
    run("chunk", "--council", "EDE", "--since", "2026-03-31")
    assert read_lines(minutes_path) == []
    run("chunk", "--council", "EDE", "--until", "2026-03-30")
    assert len(read_lines(minutes_path)) > 1
