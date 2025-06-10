import json
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from lgsf.storage import CodeCommitStorage, StorageConfig

@pytest.fixture
def mock_codecommit():
    """Create a mock CodeCommit client"""
    with patch('boto3.client') as mock_client:
        mock_codecommit = MagicMock()
        mock_client.return_value = mock_codecommit
        yield mock_codecommit

@pytest.fixture
def storage(mock_codecommit):
    """Create a CodeCommitStorage instance with mocked client"""
    config = StorageConfig(base_path="/test", council_id="TEST")
    return CodeCommitStorage(config)

def test_init(storage, mock_codecommit):
    """Test initialization of CodeCommitStorage"""
    assert storage.repository == "CouncillorsRepo"
    assert storage.branch == "master"
    assert storage.put_files == []
    mock_codecommit.get_branch.assert_called_once_with(
        repositoryName="CouncillorsRepo",
        branchName="master"
    )

def test_save_text(storage):
    """Test saving text content"""
    content = "test content"
    storage.save("test.txt", content)
    assert len(storage.put_files) == 1
    assert storage.put_files[0]["filePath"] == "test.txt"
    assert storage.put_files[0]["fileContent"] == content.encode('utf-8')

def test_save_json(storage):
    """Test saving JSON content"""
    content = {"key": "value", "list": [1, 2, 3]}
    storage.save("test.json", content)
    assert len(storage.put_files) == 1
    assert storage.put_files[0]["filePath"] == "test.json"
    assert json.loads(storage.put_files[0]["fileContent"].decode('utf-8')) == content

def test_load_existing_file(storage, mock_codecommit):
    """Test loading an existing file"""
    content = "test content"
    mock_codecommit.get_file.return_value = {
        "fileContent": content.encode('utf-8')
    }
    loaded = storage.load("test.txt")
    assert loaded == content
    mock_codecommit.get_file.assert_called_once_with(
        repositoryName="CouncillorsRepo",
        commitSpecifier=storage.branch_head,
        filePath="test.txt"
    )

def test_load_nonexistent_file(storage, mock_codecommit):
    """Test loading a nonexistent file"""
    mock_codecommit.get_file.side_effect = ClientError(
        {"Error": {"Code": "FileDoesNotExistException"}},
        "get_file"
    )
    assert storage.load("nonexistent.txt") is None

def test_delete(storage):
    """Test deleting a file"""
    storage.delete("test.txt")
    assert len(storage.put_files) == 1
    assert storage.put_files[0]["filePath"] == "test.txt"
    assert storage.put_files[0]["fileContent"] is None

def test_exists(storage, mock_codecommit):
    """Test checking if a file exists"""
    mock_codecommit.get_file.return_value = {"fileContent": b"content"}
    assert storage.exists("test.txt")
    mock_codecommit.get_file.assert_called_once_with(
        repositoryName="CouncillorsRepo",
        commitSpecifier=storage.branch_head,
        filePath="test.txt"
    )

def test_exists_nonexistent(storage, mock_codecommit):
    """Test checking if a nonexistent file exists"""
    mock_codecommit.get_file.side_effect = ClientError(
        {"Error": {"Code": "FileDoesNotExistException"}},
        "get_file"
    )
    assert not storage.exists("nonexistent.txt")

def test_commit(storage, mock_codecommit):
    """Test committing changes"""
    mock_codecommit.create_commit.return_value = {
        "commitId": "new-commit-id",
        "filesAdded": [{"blobId": "blob-id", "absolutePath": "test.txt"}]
    }
    storage.save("test.txt", "content")
    response = storage.commit("Test commit")
    assert response["commitId"] == "new-commit-id"
    assert storage.branch_head == "new-commit-id"
    assert storage.put_files == []
    mock_codecommit.create_commit.assert_called_once()

def test_get_files(storage, mock_codecommit):
    """Test getting files with a prefix"""
    mock_codecommit.get_folder.return_value = {
        "files": [
            {"absolutePath": "test/file1.txt"},
            {"absolutePath": "test/file2.txt"}
        ]
    }
    commit_id, files = storage.get_files("test")
    assert commit_id == storage.branch_head
    assert files == ["test/file1.txt", "test/file2.txt"]

def test_delete_existing(storage, mock_codecommit):
    """Test deleting existing files"""
    mock_codecommit.get_folder.return_value = {
        "files": [
            {"absolutePath": "test/file1.txt"},
            {"absolutePath": "test/file2.txt"}
        ]
    }
    mock_codecommit.create_commit.return_value = {
        "commitId": "new-commit-id"
    }
    commit_id = storage.delete_existing("test")
    assert commit_id == "new-commit-id"
    assert len(mock_codecommit.create_commit.call_args_list) == 1 