import json
import tempfile
from pathlib import Path

import pytest

from lgsf.councillors import CouncillorBase
from lgsf.storage import CouncillorStorage, StorageConfig

@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

@pytest.fixture
def storage(temp_dir):
    """Create a CouncillorStorage instance with a temporary directory"""
    config = StorageConfig(base_path=temp_dir, council_id="TEST")
    return CouncillorStorage(config)

@pytest.fixture
def sample_councillor():
    """Create a sample councillor for testing"""
    c = CouncillorBase(
        url="http://example.com/councillor",
        identifier="COUN123",
        name="John Smith",
        party="Labour",
        division="Ward 1",
    )
    c.email = "john.smith@example.com"
    c.photo_url = "http://example.com/photo.jpg"
    return c

def test_save_councillor(storage, sample_councillor):
    """Test saving a councillor"""
    storage.save("test.json", sample_councillor)
    assert len(storage.councillors) == 1
    assert sample_councillor in storage.councillors

def test_save_invalid_content(storage):
    """Test saving invalid content"""
    with pytest.raises(ValueError):
        storage.save("test.json", "not a councillor")

def test_load_councillor(storage, sample_councillor, temp_dir):
    """Test loading a councillor from file"""
    # Create a JSON file with councillor data
    file_path = Path(temp_dir) / "test.json"
    with open(file_path, "w") as f:
        json.dump(sample_councillor.as_dict(), f)
    
    loaded = storage.load(str(file_path))
    assert loaded is not None
    assert loaded.url == sample_councillor.url
    assert loaded.identifier == sample_councillor.identifier
    assert loaded.name == sample_councillor.name
    assert loaded.party == sample_councillor.party
    assert loaded.division == sample_councillor.division
    assert loaded.email == sample_councillor.email
    assert loaded.photo_url == sample_councillor.photo_url

def test_load_nonexistent_file(storage):
    """Test loading a nonexistent file"""
    assert storage.load("nonexistent.json") is None

def test_load_invalid_json(storage, temp_dir):
    """Test loading invalid JSON"""
    file_path = Path(temp_dir) / "invalid.json"
    with open(file_path, "w") as f:
        f.write("invalid json")
    
    assert storage.load(str(file_path)) is None

def test_get_all_councillors(storage, sample_councillor):
    """Test getting all councillors"""
    storage.save("test1.json", sample_councillor)
    storage.save("test2.json", sample_councillor)
    councillors = storage.get_all_councillors()
    assert len(councillors) == 2
    assert all(isinstance(c, CouncillorBase) for c in councillors)

def test_export_to_csv(storage, sample_councillor, temp_dir):
    """Test exporting councillors to CSV"""
    storage.save("test.json", sample_councillor)
    csv_path = Path(temp_dir) / "output.csv"
    storage.export_to_csv(str(csv_path))
    
    assert csv_path.exists()
    with open(csv_path) as f:
        content = f.read()
        assert "identifier,name,party,division" in content
        assert "COUN123,John Smith,Labour,Ward 1" in content

def test_councillor_data_methods(sample_councillor):
    """Test CouncillorBase methods"""
    # Test as_file_name
    assert sample_councillor.as_file_name() == "coun123-john-smith"
    
    # Test as_dict
    data = sample_councillor.as_dict()
    assert data["url"] == sample_councillor.url
    assert data["raw_identifier"] == sample_councillor.identifier
    assert data["raw_name"] == sample_councillor.name
    assert data["raw_party"] == sample_councillor.party
    assert data["raw_division"] == sample_councillor.division
    assert data["email"] == sample_councillor.email
    assert data["photo_url"] == sample_councillor.photo_url
    
    # Test as_json
    json_str = sample_councillor.as_json()
    assert isinstance(json_str, str)
    loaded_data = json.loads(json_str)
    assert loaded_data == data
    
    # Test as_csv
    csv_str = sample_councillor.as_csv()
    assert "COUN123,John Smith,Labour,Ward 1" in csv_str

def test_councillor_data_from_file_name(tmp_path):
    """Test creating CouncillorBase from file name"""
    data = {
        "url": "http://example.com/councillor",
        "raw_identifier": "COUN123",
        "raw_name": "John Smith",
        "raw_party": "Labour",
        "raw_division": "Ward 1",
        "email": "john.smith@example.com",
        "photo_url": "http://example.com/photo.jpg"
    }
    file_path = tmp_path / "coun123-john-smith.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
    councillor = CouncillorBase.from_file_name(file_path)
    assert councillor.url == data["url"]
    assert councillor.identifier == data["raw_identifier"]
    assert councillor.name == data["raw_name"]
    assert councillor.party == data["raw_party"]
    assert councillor.division == data["raw_division"]
    assert councillor.email == data["email"]
    assert councillor.photo_url == data["photo_url"] 