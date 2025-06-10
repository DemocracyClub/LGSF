import json
import os
import tempfile
from pathlib import Path

import pytest

from lgsf.storage import FileSystemStorage, StorageConfig

@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

@pytest.fixture
def storage(temp_dir):
    """Create a FileSystemStorage instance with a temporary directory"""
    config = StorageConfig(base_path=temp_dir, council_id="TEST")
    return FileSystemStorage(config)

def test_save_and_load_text(storage):
    """Test saving and loading text content"""
    content = "test content"
    storage.save("test.txt", content)
    loaded = storage.load("test.txt")
    assert loaded == content

def test_save_and_load_json(storage):
    """Test saving and loading JSON content"""
    content = {"key": "value", "list": [1, 2, 3]}
    storage.save("test.json", content)
    loaded = storage.load("test.json")
    assert loaded == content

def test_save_and_load_nested_path(storage):
    """Test saving and loading with nested paths"""
    content = "nested content"
    storage.save("nested/path/test.txt", content)
    loaded = storage.load("nested/path/test.txt")
    assert loaded == content
    assert os.path.exists(os.path.join(storage.config.base_path, "nested/path/test.txt"))

def test_delete(storage):
    """Test deleting files"""
    content = "test content"
    storage.save("test.txt", content)
    assert storage.exists("test.txt")
    storage.delete("test.txt")
    assert not storage.exists("test.txt")

def test_exists(storage):
    """Test checking if files exist"""
    assert not storage.exists("nonexistent.txt")
    storage.save("test.txt", "content")
    assert storage.exists("test.txt")

def test_clean_directory(storage):
    """Test cleaning a directory"""
    # Create some test files and directories
    storage.save("file1.txt", "content1")
    storage.save("dir1/file2.txt", "content2")
    storage.save("dir1/dir2/file3.txt", "content3")
    
    # Clean the directory
    storage.clean_directory("")
    
    # Check that everything is gone
    assert not storage.exists("file1.txt")
    assert not storage.exists("dir1/file2.txt")
    assert not storage.exists("dir1/dir2/file3.txt")
    assert not os.path.exists(os.path.join(storage.config.base_path, "dir1"))

def test_load_nonexistent_file(storage):
    """Test loading a nonexistent file"""
    assert storage.load("nonexistent.txt") is None
    assert storage.load("nonexistent.json") is None 