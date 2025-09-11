import abc
from pathlib import Path


class BaseStorage(abc.ABC):
    def touch(self, filename: Path):
        return self._touch(filename)

    @abc.abstractmethod
    def _touch(self, filename: Path): ...

    def write(self, filename: Path, content: str):
        return self._write(filename, content)

    @abc.abstractmethod
    def _write(self, filename: Path, content: str): ...

    def open(self, filename: Path, mode="r"):
        return self._open(filename, mode=mode)

    @abc.abstractmethod
    def _open(self, filename: Path, mode: str): ...


class LocalFileStorage(BaseStorage):
    def _touch(self, file: Path):
        return file.touch()

    def _write(self, file: Path, content: str):
        return file.write_text(content)

    def _open(self, filename: Path, mode: str = "f"):
        return filename.open(mode=mode)


def get_storage_backend() -> BaseStorage:
    # TODO: find a way to take this from settings
    return LocalFileStorage()


if __name__ == "__main__":
    print(get_storage_backend())
