from abc import ABC, abstractmethod


class StorageEngine(ABC):
    @abstractmethod
    def set(self, key: bytes, value: bytes, /) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, key: bytes, /) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: bytes, /) -> None:
        raise NotImplementedError


class Index(ABC):
    @abstractmethod
    def has(self, key: bytes, /) -> bool:
        raise NotImplementedError

    @abstractmethod
    def set(self, key: bytes, offset: int, /) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, key: bytes, /) -> int:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: bytes, /) -> None:
        raise NotImplementedError
