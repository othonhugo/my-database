import pytest
from tempfile import mkstemp

from mydb.storage.logger import AppendOnlyLogStorage


@pytest.fixture
def log_filepath() -> str:
    """Provides a path to a temporary and isolated log file for each test, ensuring that tests do not interfere with one another."""

    _, filepath = mkstemp(prefix="mydb_", suffix="_test")

    return filepath


@pytest.fixture
def populated_storage(log_filepath: str) -> AppendOnlyLogStorage:
    """Provides a Storage instance that is pre-populated with a known dataset, useful for testing read, update, and delete operations."""

    storage = AppendOnlyLogStorage(log_filepath)

    storage.set(b"key1", b"value1")
    storage.set(b"key2", b"value2")
    storage.set(b"key-to-update", b"initial-value")
    storage.set(b"key-to-delete", b"doomed-value")
    storage.set(b"key-to-update", b"final-value")

    return storage
