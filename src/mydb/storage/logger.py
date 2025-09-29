from dataclasses import dataclass
from struct import Struct
from typing import IO, Self

from storage.engine import StorageEngine

BytesIO = IO[bytes]


class NonExistentKeyError(Exception):
    pass


class StructFormatter:
    def __init__(self, format: str) -> None:
        self.format = format

    def create_struct(self, *args: object, **kwargs: object) -> Struct:
        return Struct(self.format.format(*args, **kwargs))


def unpack_from_stream(struct: Struct, stream: BytesIO, /) -> tuple | None:
    raw = stream.read(struct.size)

    if len(raw) < struct.size:
        return None

    return struct.unpack(raw)


@dataclass
class AppendOnlyLogHeader:
    class Metadata:
        IS_ACTIVE = Struct("?")
        KEY_SIZE = Struct("Q")
        VALUE_SIZE = Struct("Q")

    key_size: int
    value_size: int
    is_active: bool = True

    def to_stream(self, stream: BytesIO, /) -> int:
        count = stream.write(self.Metadata.IS_ACTIVE.pack(self.is_active))
        count += stream.write(self.Metadata.KEY_SIZE.pack(self.key_size))
        count += stream.write(self.Metadata.VALUE_SIZE.pack(self.value_size))

        return count

    @classmethod
    def from_stream(cls, stream: BytesIO, /) -> Self | None:
        # WARNING: note the extended unpacking

        is_active_raw = unpack_from_stream(cls.Metadata.IS_ACTIVE, stream)

        if is_active_raw is None:
            return None

        is_active, = is_active_raw

        key_size_raw = unpack_from_stream(cls.Metadata.KEY_SIZE, stream)

        if key_size_raw is None:
            return None

        key_size, = key_size_raw

        value_size_raw = unpack_from_stream(cls.Metadata.VALUE_SIZE, stream)

        if value_size_raw is None:
            return None

        value_size, = value_size_raw

        return cls(
            is_active=is_active,
            key_size=key_size,
            value_size=value_size
        )


@dataclass
class AppendOnlyLogPayload:
    class Metadata:
        KEY = StructFormatter("{key_size}s")
        VALUE = StructFormatter("{value_size}s")

    key: bytes
    value: bytes

    def to_stream(self, stream: BytesIO, /) -> int:
        key_meta = self.Metadata.KEY.create_struct(key_size=len(self.key))
        value_meta = self.Metadata.VALUE.create_struct(value_size=len(self.value))

        count = stream.write(key_meta.pack(self.key))
        count += stream.write(value_meta.pack(self.value))

        return count

    @classmethod
    def from_stream(cls, stream: BytesIO, /, *, key_size: int, value_size: int) -> Self | None:
        # WARNING: note the extended unpacking

        key_meta = cls.Metadata.KEY.create_struct(key_size=key_size)
        key_raw = unpack_from_stream(key_meta, stream)

        if key_raw is None:
            return None

        key, = key_raw

        value_meta = cls.Metadata.VALUE.create_struct(value_size=value_size)
        value_raw = unpack_from_stream(value_meta, stream)

        if value_raw is None:
            return None

        value, = value_raw

        return cls(
            key=key,
            value=value
        )


@dataclass
class AppendOnlyLogRecord:
    header: AppendOnlyLogHeader
    payload: AppendOnlyLogPayload

    def to_stream(self, stream: BytesIO, /) -> int:
        count = self.header.to_stream(stream)
        count += self.payload.to_stream(stream)

        return count

    @classmethod
    def from_stream(cls, stream: BytesIO, /) -> Self | None:
        header = AppendOnlyLogHeader.from_stream(stream)

        if header is None:
            return None

        payload = AppendOnlyLogPayload.from_stream(
            stream, key_size=header.key_size, value_size=header.value_size)

        if payload is None:
            return None

        return cls(
            header=header,
            payload=payload
        )


class AppendOnlyLogStorage(StorageEngine):
    def __init__(self, filepath: str) -> None:
        self.filepath = filepath

    def set(self, key: bytes, value: bytes, /) -> None:
        header = AppendOnlyLogHeader(
            key_size=len(key), value_size=len(value)
        )

        payload = AppendOnlyLogPayload(
            key=key, value=value
        )

        record = AppendOnlyLogRecord(
            header=header, payload=payload
        )

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
