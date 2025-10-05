import pytest

EDGE_CASE_DATA = [
    pytest.param(b"empty-value-key", b"", id="empty_value"),
    pytest.param(b"", b"empty-key-value", id="empty_key"),
    pytest.param(b"", b"", id="empty_key_and_value"),
    pytest.param(b"long-key-" * 100, b"short", id="long_key"),
    pytest.param(b"short", b"long-value-" * 1000, id="long_value"),
    pytest.param(b"key\x00\xff", b"value\x80\x7f", id="binary_data"),
]
