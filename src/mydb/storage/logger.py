from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from struct import Struct
from typing import BinaryIO, Self

from mydb.core import MyDBError
from mydb.storage.abstract import StorageEngine


class AppendOnlyLogOperation(IntEnum):
    SET = 0
    DELETE = 1


class LogStorageError(MyDBError):
    """Base exception for log storage errors."""


class LogKeyNotFoundError(LogStorageError):
    """Raised when a key is not found in the log."""

    def __init__(self, *, key: bytes):
        self.key = key

        super().__init__(f"Key not found: {key!r}")


class LogCorruptedError(LogStorageError):
    """Raised when the log file appears to be corrupted."""

    def __init__(self, *, offset: int, cause: str | Exception | None = None):
        self.offset = offset
        self.cause = cause

        message = f"Log corrupted at offset {offset}"

        if cause:
            message += f": {cause}"

        super().__init__(message)


@dataclass(frozen=True)
class AppendOnlyLogHeader:
    STRUCT = Struct("BQQ")

    operation: AppendOnlyLogOperation
    key_size: int
    value_size: int

    @property
    def payload_size(self) -> int:
        return self.key_size + self.value_size

    @property
    def record_size(self) -> int:
        return self.STRUCT.size + self.payload_size

    def to_bytes(self) -> bytes:
        return self.STRUCT.pack(self.operation.value, self.key_size, self.value_size)

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        op_value, key_size, value_size = cls.STRUCT.unpack(data)

        try:
            operation = AppendOnlyLogOperation(op_value)
        except ValueError as e:
            raise LogStorageError(e) from e

        return cls(operation=operation, key_size=key_size, value_size=value_size)


@dataclass(frozen=True)
class AppendOnlyLogPayload:
    key: bytes
    value: bytes

    def to_bytes(self) -> bytes:
        return self.key + self.value


@dataclass(frozen=True)
class AppendOnlyLogRecord:
    header: AppendOnlyLogHeader
    payload: AppendOnlyLogPayload

    def to_stream(self, stream: BinaryIO, /) -> int:
        count = stream.write(self.header.to_bytes())
        count += stream.write(self.payload.to_bytes())

        return count

    @classmethod
    def from_stream(cls, stream: BinaryIO, /) -> Self | None:
        offset = stream.tell()

        if not (header_bytes := stream.read(AppendOnlyLogHeader.STRUCT.size)):
            return None

        if len(header_bytes) < AppendOnlyLogHeader.STRUCT.size:
            raise LogCorruptedError(offset=offset, cause="Truncated record header.")

        try:
            header = AppendOnlyLogHeader.from_bytes(header_bytes)

            payload_bytes = stream.read(header.payload_size)

            if len(payload_bytes) != header.payload_size:
                raise LogCorruptedError(offset=offset, cause="Truncated record payload.")

            payload_struct = Struct(f"{header.key_size}s{header.value_size}s")

            key_bytes, value_bytes = payload_struct.unpack(payload_bytes)

            payload = AppendOnlyLogPayload(key=key_bytes, value=value_bytes)

            return cls(header=header, payload=payload)
        except Exception as e:
            raise LogCorruptedError(offset=offset, cause=e) from e


class AppendOnlyLogStorage(StorageEngine):
    def __init__(self, filepath: str) -> None:
        self.filepath = Path(filepath)

        self.filepath.touch()

    def get(self, key: bytes, /) -> bytes:
        _, value = self._resolve_latest_entry(key)

        return value

    def set(self, key: bytes, value: bytes, /) -> None:
        self._append_record(AppendOnlyLogOperation.SET, key, value)

    def delete(self, key: bytes, /) -> None:
        self._append_record(AppendOnlyLogOperation.DELETE, key, b"")

    def _append_record(self, operation: AppendOnlyLogOperation, key: bytes, value: bytes) -> None:
        header = AppendOnlyLogHeader(operation=operation, key_size=len(key), value_size=len(value))
        payload = AppendOnlyLogPayload(key=key, value=value)
        record = AppendOnlyLogRecord(header=header, payload=payload)

        with open(self.filepath, "ab") as f:
            record.to_stream(f)

    def _resolve_latest_entry(self, key: bytes, /) -> tuple[int, bytes]:
        latest_value = None
        latest_offset = -1

        with open(self.filepath, "rb") as f:
            while True:
                current_offset = f.tell()

                record = AppendOnlyLogRecord.from_stream(f)

                if record is None:
                    break

                if record.payload.key == key:
                    if record.header.operation is AppendOnlyLogOperation.DELETE:
                        latest_value = None
                        latest_offset = -1
                    else:
                        latest_value = record.payload.value
                        latest_offset = current_offset

        if latest_value is not None:
            return latest_offset, latest_value

        raise LogKeyNotFoundError(key=key)
