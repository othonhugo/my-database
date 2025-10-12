import pytest

from mydb.storage.index import InMemoryIndex


@pytest.fixture
def in_memory_index() -> InMemoryIndex:
    """Returns a new, empty InMemoryIndex instance for each test."""

    return InMemoryIndex()


EDGE_SCENARIOS = [
    # fmt: off

    # A zero-length key, the most fundamental edge case.
    pytest.param(
        b"",
        id="empty-key"
    ),

    # A key containing only a single space.
    pytest.param(
        b" ",
        id="single-space-key"
    ),

    # Ensures leading/trailing whitespace is treated as part of the key, not trimmed.
    pytest.param(
        b"  leading-and-trailing-spaces  ",
        id="key-with-whitespace"
    ),

    # Proves the system is "binary safe" by handling null bytes correctly.
    pytest.param(
        b"key\x00with\x00nulls",
        id="key-with-null-bytes"
    ),

    # A key made of arbitrary non-printable bytes to test "8-bit clean" handling.
    pytest.param(
        b"\xde\xad\xbe\xef",
        id="purely-binary-key"
    ),

    # Verifies that control characters like newlines are handled literally.
    pytest.param(
        b"key\nwith\r\nnewlines",
        id="key-with-control-chars"
    ),

    # A key with byte values outside the standard 7-bit ASCII range.
    pytest.param(
        b"\xff\xfe\xfd",
        id="key-with-high-bytes"
    ),

    # The smallest possible non-empty key.
    pytest.param(
        b"A",
        id="single-byte-key"
    ),

    # A large key (4KB) to test for performance or buffer-related issues.
    pytest.param(
        b"A" * 4096,
        id="long-key-4kb"
    ),

    # A key containing multi-byte UTF-8 characters.
    pytest.param(
        "chave-com-acentuação-ç".encode("utf-8"),
        id="utf8-encoded-key"
    ),

    # A key with various symbols that might be special in other parsing contexts.
    pytest.param(
        b'key-with-"quotes"-and-symbols/\\!@#$%',
        id="key-with-special-symbols"
    ),
]

# Core Functionality and Lifecycle Tests


def test_set_new_key_can_be_retrieved(in_memory_index: InMemoryIndex):
    """
    Sets a key with a specific offset for the first time.

    Verifies that a new entry is correctly stored and its offset can be retrieved immediately.
    """


def test_set_existing_key_updates_offset(in_memory_index: InMemoryIndex):
    """
    Sets a new offset for a key that already exists.

    Ensures the index correctly updates the offset for an existing key, following last-write-wins semantics.
    """


def test_has_returns_true_for_existing_key(in_memory_index: InMemoryIndex):
    """
    Calls the `has()` method for a key known to be in the index.

    Confirms that the presence check correctly returns True for an existing key.
    """


def test_deleted_key_is_no_longer_accessible(in_memory_index: InMemoryIndex):
    """
    Deletes a key that was previously present in the index.

    Verifies that after deletion, the key is inaccessible via `get()` and `has()` returns False.
    """


# Error Handling and Behavior for Non-existent Keys


def test_get_nonexistent_key_raises_error(in_memory_index: InMemoryIndex):
    """
    Attempts to `get()` a key that has never been set.

    Ensures that accessing a non-existent key raises the specific `InMemoryIndexKeyNotFoundError`.
    """


def test_has_returns_false_for_nonexistent_key(in_memory_index: InMemoryIndex):
    """
    Calls the `has()` method for a key known to not be in the index.

    Confirms that the presence check correctly returns False for a non-existent key.
    """


def test_delete_nonexistent_key_is_silent_and_idempotent(in_memory_index: InMemoryIndex):
    """
    Calls `delete()` on a key that is not present in the index.

    Verifies that this operation is a safe no-op (idempotent) and does not raise an error.
    """


# Edge Case Tests using Parameterization


@pytest.mark.parametrize("key", EDGE_SCENARIOS)
def test_lifecycle_with_edge_case_keys(in_memory_index: InMemoryIndex, key: bytes):
    """
    Runs the full set -> update -> delete -> get lifecycle for a given key.

    This test is parameterized to ensure the index handles various edge-case key formats
    (e.g., empty, binary, large) robustly across all its core operations.
    """
