from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from struct import Struct
from struct import error as StructError
from typing import BinaryIO, Self

from core import MyDBError
from storage.engine import StorageEngine


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
    STRUCT = Struct("<BQQ")

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
        return self.STRUCT.pack(self.operation, self.key_size, self.value_size)

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        op_value, key_size, value_size = cls.STRUCT.unpack(data)

        operation = AppendOnlyLogOperation(op_value)

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
        except StructError as e:
            raise LogCorruptedError(offset=offset, cause=e) from e


class AppendOnlyLogStorage(StorageEngine):
    def __init__(self, filepath: str) -> None:
        self.filepath = filepath

    def set(self, key: bytes, value: bytes, /) -> None:
        header = AppendOnlyLogHeader(key_size=len(key), value_size=len(value))
        payload = AppendOnlyLogPayload(key=key, value=value)
        record = AppendOnlyLogRecord(header=header, payload=payload)

        with open(self.filepath, "ab") as f:
            record.to_stream(f)

    def get(self, key: bytes, /) -> bytes:
        with open(self.filepath, "rb") as f:
            while True:
                header = AppendOnlyLogHeader.from_stream(f)

                if header is None:
                    break

                payload = AppendOnlyLogPayload.from_stream(
                    f, key_size=header.key_size, value_size=header.value_size
                )

                if payload is None:
                    break

                if payload.key == key:
                    return payload.value

        raise NonExistentKeyError(f"Key does not exist: {key!r}")

    def pop(self, key: bytes, /) -> bytes:
        raise NotImplementedError
