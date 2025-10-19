import pytest

from mydb.storage.index import InMemoryIndex, InMemoryIndexKeyNotFoundError


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

# A constant for testing large offsets, representing the 4GB file size boundary.
LARGE_OFFSET = 2**32

# A comprehensive collection of scenarios combining edge-case keys with edge-case offsets.
BASE_SCENARIOS = [
    # fmt: off

    # A standard key at offset 0, the very beginning of the file.
    pytest.param(
        b"standard-key", 0,
        id="standard-key-offset-zero"
    ),

    # A standard key with a very large offset to test handling of large files.
    pytest.param(
        b"standard-key", LARGE_OFFSET,
        id="standard-key-large-offset"
    ),

    # A zero-length key, the most fundamental edge case.
    pytest.param(
        b"", 12345,
        id="empty-key"
    ),

    # A key containing only a single space.
    pytest.param(
        b" ", 12345,
        id="single-space-key"
    ),

    # Ensures leading/trailing whitespace is treated as part of the key, not trimmed.
    pytest.param(
        b"  leading-and-trailing-spaces  ", 12345,
        id="key-with-whitespace"
    ),

    # Proves the system is "binary safe" by handling null bytes correctly.
    pytest.param(
        b"key\x00with\x00nulls", 12345,
        id="key-with-null-bytes"
    ),

    # A key made of arbitrary non-printable bytes to test "8-bit clean" handling.
    pytest.param(
        b"\xde\xad\xbe\xef", 12345,
        id="purely-binary-key"
    ),

    # Verifies that control characters like newlines are handled literally.
    pytest.param(
        b"key\nwith\r\nnewlines", 12345,
        id="key-with-control-chars"
    ),

    # A key with byte values outside the standard 7-bit ASCII range.
    pytest.param(
        b"\xff\xfe\xfd", 12345,
        id="key-with-high-bytes"
    ),

    # The smallest possible non-empty key.
    pytest.param(
        b"A", 12345,
        id="single-byte-key"
    ),

    # A large key (4KB) to test for performance or buffer-related issues.
    pytest.param(b"A" * 4096, 12345,
        id="long-key-4kb"
    ),

    # A key containing multi-byte UTF-8 characters.
    pytest.param(
        "chave-com-acentuação-ç".encode("utf-8"), 12345,
        id="utf8-encoded-key"
    ),

    # A key with various symbols that might be special in other parsing contexts.
    pytest.param(
        b'key-with-"quotes"-and-symbols/\\!@#$%', 12345,
        id="key-with-special-symbols"
    ),
]

# A comprehensive collection of scenarios for testing the update (overwrite) logic.
UPDATE_SCENARIOS = [
    # fmt: off

    # The baseline "happy path" for an update, with a standard key.
    pytest.param(
        b"standard-key", 100, 500,
        id="standard-update"
    ),

    # An update where the initial record was at the very beginning of the file.
    pytest.param(
        b"key-at-zero", 0, 250,
        id="update-from-offset-zero"
    ),

    # An update to a very large offset, simulating a large log file.
    pytest.param(
        b"key-with-large-offset", 200, LARGE_OFFSET,
        id="update-to-large-offset"
    ),

    *(pytest.param(
        p.values[0],          # The key from the original scenario
        123,                  # A standard initial offset
        456,                  # A standard updated offset
        id=f"{p.id}-update"   # Append '-update' to the original ID for clarity
    ) for p in BASE_SCENARIOS),
]

# Core Functionality and Lifecycle Tests


@pytest.mark.parametrize("key, offset", BASE_SCENARIOS)
def test_set_new_key_can_be_retrieved(in_memory_index: InMemoryIndex, key: bytes, offset: int):
    """
    Sets a key with a specific offset for the first time.

    Verifies that a new entry is correctly stored and its offset can be retrieved immediately.
    """

    # ARRANGE
    index = in_memory_index

    # ACT
    index.set(key, offset)
    retrieved_offset = index.get(key)

    # ASSERT
    assert retrieved_offset == offset


@pytest.mark.parametrize("key, initial_offset, updated_offset", UPDATE_SCENARIOS)
def test_set_existing_key_updates_offset(in_memory_index: InMemoryIndex, key: bytes, initial_offset: int, updated_offset: int):
    """
    Sets a new offset for a key that already exists.

    Ensures the index correctly updates the offset for an existing key, following last-write-wins semantics.
    """

    # ARRANGE
    index = in_memory_index

    # ACT
    index.set(key, initial_offset)
    index.set(key, updated_offset)
    retrieved_offset = index.get(key)

    # ASSERT
    assert retrieved_offset == updated_offset


@pytest.mark.parametrize("key, offset", BASE_SCENARIOS)
def test_has_returns_true_for_existing_key(in_memory_index: InMemoryIndex, key: bytes, offset: int):
    """
    Calls the `has()` method for a key known to be in the index.

    Confirms that the presence check correctly returns True for an existing key.
    """

    # ARRANGE
    index = in_memory_index

    # ACT
    index.set(key, offset)

    # ASSERT
    assert index.has(key) is True


@pytest.mark.parametrize("key, offset", BASE_SCENARIOS)
def test_deleted_key_is_no_longer_accessible(in_memory_index: InMemoryIndex, key: bytes, offset: int):
    """
    Deletes a key that was previously present in the index.

    Verifies that after deletion, the key is inaccessible via `get()` and `has()` returns False.
    """

    # ARRANGE
    index = in_memory_index

    # ACT
    index.set(key, offset)
    index.delete(key)

    # ASSERT
    assert index.has(key) is False

    with pytest.raises(InMemoryIndexKeyNotFoundError) as exc_info:
        index.get(key)

    assert exc_info.value.key == key


@pytest.mark.parametrize("key, _", BASE_SCENARIOS)
def test_get_nonexistent_key_raises_error(in_memory_index: InMemoryIndex, key: bytes, _: int):
    """
    Attempts to `get()` a key that has never been set.

    Ensures that accessing a non-existent key raises the specific `InMemoryIndexKeyNotFoundError`.
    """

    # ARRANGE
    index = in_memory_index

    # ACT & ASSERT
    with pytest.raises(InMemoryIndexKeyNotFoundError) as exc_info:
        index.get(key)

    assert exc_info.value.key == key


@pytest.mark.parametrize("key, _", BASE_SCENARIOS)
def test_has_returns_false_for_nonexistent_key(in_memory_index: InMemoryIndex, key: bytes, _: int):
    """
    Calls the `has()` method for a key known to not be in the index.

    Confirms that the presence check correctly returns False for a non-existent key.
    """

    # ARRANGE
    index = in_memory_index

    # ACT & ASSERT
    assert index.has(key) is False


@pytest.mark.parametrize("key, _", BASE_SCENARIOS)
def test_delete_nonexistent_key_is_silent_and_idempotent(in_memory_index: InMemoryIndex, key: bytes, _: int):
    """
    Calls `delete()` on a key that is not present in the index.

    Verifies that this operation is a safe no-op (idempotent) and does not raise an error.
    """

    # ARRANGE
    index = in_memory_index

    # ACT & ASSERT
    try:
        index.delete(key)
    except Exception as e:
        pytest.fail(f"Deleting a non-existent key raised an unexpected exception: {e}")


# Edge Case Tests using Parameterization


@pytest.mark.parametrize("key", EDGE_SCENARIOS)
def test_lifecycle_with_edge_case_keys(in_memory_index: InMemoryIndex, key: bytes):
    """
    Runs the full set -> update -> delete -> get lifecycle for a given key.

    This test is parameterized to ensure the index handles various edge-case key formats
    (e.g., empty, binary, large) robustly across all its core operations.
    """
