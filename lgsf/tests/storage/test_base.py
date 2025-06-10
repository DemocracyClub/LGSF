import pytest
from lgsf.storage import StorageBackend, StorageConfig

def test_storage_config():
    """Test StorageConfig initialization and defaults"""
    config = StorageConfig(base_path="/test/path", council_id="TEST")
    assert config.base_path == "/test/path"
    assert config.council_id == "TEST"
    assert config.verbose is False

    config = StorageConfig(base_path="/test/path", council_id="TEST", verbose=True)
    assert config.verbose is True

def test_storage_backend_abstract():
    """Test that StorageBackend cannot be instantiated directly"""
    with pytest.raises(TypeError):
        StorageBackend(StorageConfig(base_path="/test", council_id="TEST")) 