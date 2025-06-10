import json
from unittest.mock import MagicMock, patch
import pytest
from github import GithubException
from lgsf.storage.github import GitHubStorage
from lgsf.storage.base import StorageConfig

@pytest.fixture
def mock_repo():
    with patch('lgsf.storage.github.Github') as mock_github:
        mock_repo = MagicMock()
        mock_github.return_value.get_repo.return_value = mock_repo
        yield mock_repo

@pytest.fixture
def storage(mock_repo):
    config = StorageConfig(base_path="/test", council_id="TEST")
    config.github_token = "fake-token"
    config.github_repo = "symroe/test-repo"
    config.github_branch = "main"
    return GitHubStorage(config)

def test_save_text(storage):
    content = "test content"
    storage.save("test.txt", content)
    assert len(storage.put_files) == 1
    assert storage.put_files[0]["filePath"] == "test.txt"
    assert storage.put_files[0]["fileContent"] == content.encode('utf-8')

def test_save_json(storage):
    content = {"key": "value", "list": [1, 2, 3]}
    storage.save("test.json", content)
    assert len(storage.put_files) == 1
    assert storage.put_files[0]["filePath"] == "test.json"
    assert json.loads(storage.put_files[0]["fileContent"].decode('utf-8')) == content

def test_load_existing_file(storage, mock_repo):
    mock_file = MagicMock()
    mock_file.decoded_content = b"test content"
    mock_repo.get_contents.return_value = mock_file
    loaded = storage.load("test.txt")
    assert loaded == "test content"
    mock_repo.get_contents.assert_called_once_with("test.txt", ref="main")

def test_load_nonexistent_file(storage, mock_repo):
    mock_repo.get_contents.side_effect = GithubException(404, "Not Found", None)
    assert storage.load("nonexistent.txt") is None

def test_delete(storage):
    storage.delete("test.txt")
    assert len(storage.put_files) == 1
    assert storage.put_files[0]["filePath"] == "test.txt"
    assert storage.put_files[0]["fileContent"] is None

def test_exists(storage, mock_repo):
    mock_repo.get_contents.return_value = MagicMock()
    assert storage.exists("test.txt")
    mock_repo.get_contents.assert_called_once_with("test.txt", ref="main")

def test_exists_nonexistent(storage, mock_repo):
    mock_repo.get_contents.side_effect = GithubException(404, "Not Found", None)
    assert not storage.exists("nonexistent.txt")

def test_commit_create_and_update(storage, mock_repo):
    # Test create
    storage.save("test.txt", "content")
    mock_repo.get_contents.side_effect = GithubException(404, "Not Found", None)
    mock_repo.create_file.return_value = {"commit": {"sha": "new-sha"}}
    storage.commit("Test commit")
    mock_repo.create_file.assert_called_once()
    # Test update
    storage.save("test.txt", "content")
    mock_file = MagicMock()
    mock_file.sha = "sha"
    mock_repo.get_contents.side_effect = None
    mock_repo.get_contents.return_value = mock_file
    mock_repo.update_file.return_value = {"commit": {"sha": "updated-sha"}}
    storage.commit("Test commit")
    mock_repo.update_file.assert_called_once()

def test_commit_delete(storage, mock_repo):
    storage.delete("test.txt")
    mock_file = MagicMock()
    mock_file.sha = "sha"
    mock_repo.get_contents.return_value = mock_file
    mock_repo.delete_file.return_value = {"commit": {"sha": "deleted-sha"}}
    storage.commit("Delete commit")
    mock_repo.delete_file.assert_called_once()

def test_get_files(storage, mock_repo):
    mock_file1 = MagicMock()
    mock_file1.type = 'file'
    mock_file1.path = 'test/file1.txt'
    mock_file2 = MagicMock()
    mock_file2.type = 'file'
    mock_file2.path = 'test/file2.txt'
    mock_repo.get_contents.return_value = [mock_file1, mock_file2]
    storage._branch_ref = MagicMock()
    storage._branch_ref.object.sha = 'sha'
    commit_id, files = storage.get_files("test")
    assert commit_id == 'sha'
    assert files == ["test/file1.txt", "test/file2.txt"]

def test_delete_existing(storage, mock_repo):
    mock_file1 = MagicMock()
    mock_file1.type = 'file'
    mock_file1.path = 'test/file1.txt'
    mock_file2 = MagicMock()
    mock_file2.type = 'file'
    mock_file2.path = 'test/file2.txt'
    mock_repo.get_contents.return_value = [mock_file1, mock_file2]
    storage._branch_ref = MagicMock()
    storage._branch_ref.object.sha = 'sha'
    mock_repo.delete_file.return_value = {"commit": {"sha": "deleted-sha"}}
    commit_id = storage.delete_existing("test")
    assert commit_id == "Delete all files under test" 