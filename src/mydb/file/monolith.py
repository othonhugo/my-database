from os import SEEK_SET
from pathlib import Path

from mydb.interface import File, OpenFileMode


class MonolithicStorage(File):
    def __init__(self, tablespace: str, directory: Path | str, mode: OpenFileMode = "rb"):
        tablespace = tablespace.strip()

        if not tablespace:
            raise ValueError("Tablespace cannot be empty.")

        directory = Path(directory).resolve()

        if not directory.is_dir():
            raise ValueError("Directory does not exist")

        self._tablespace = tablespace
        self._directory = directory
        self._mode = mode

        self.path.touch(exist_ok=True)

        self._file = open(self.path, self._mode)

    @property
    def path(self) -> Path:
        filename = f"{self._tablespace}.dblog"

        return self._directory / filename

    @property
    def size(self) -> int:
        try:
            return self.path.stat().st_size
        except FileNotFoundError:
            return 0

    @property
    def closed(self) -> bool:
        return self._file.closed

    def write(self, data: bytes) -> int:
        return self._file.write(data)

    def read(self, size: int = -1) -> bytes:
        return self._file.read(size)

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        return self._file.tell()

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> "MonolithicStorage":
        self._file.__enter__()

        return self

    def __exit__(self, *_: object) -> None:
        self.close()
