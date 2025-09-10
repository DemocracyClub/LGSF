import abc
from pathlib import Path


class BaseStorage(abc.ABC):
    def touch(self, file: Path):
        return self._touch(file)

    @abc.abstractmethod
    def _touch(self, file: Path): ...

    def write(self, file: Path, content: str):
        return self._write(file, content)

    @abc.abstractmethod
    def _write(self, file: Path, content: str): ...


class LocalFileStorage(BaseStorage):
    def _touch(self, file: Path):
        return file.touch()

    def _write(self, file: Path, content: str):
        return file.write_text(content)


def get_storage_backend() -> BaseStorage:
    # TODO: find a way to take this from settings
    return LocalFileStorage()


if __name__ == "__main__":
    print(get_storage_backend())
