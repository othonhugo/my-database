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
