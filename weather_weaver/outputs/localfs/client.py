import shutil
from pathlib import Path

import dask.dataframe
import structlog

from weather_weaver.models.storage import StorageInterface

logger = structlog.getLogger()


class Client(StorageInterface):
    def exists(self, *, path: Path) -> bool:
        """Check if file exists."""
        return path.exists()

    def list_files_for_request(self, *, folder: Path, extension: str = "parquet") -> list[Path]:
        """List the available files."""
        return folder.glob(f"*.{extension}")

    def store(self, *, ddf: dask.dataframe.DataFrame, destination_path: Path) -> None:
        """Store a dataset."""
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        ddf.to_parquet(destination_path)
        logger.debug(
            event="Stored DataFrame",
            destination_path=destination_path,
        )
        return

    def delete(self, *, path: Path) -> None:
        """Delete object by path."""
        if not path.exists():
            raise FileNotFoundError(f"file does not exist: {p}")
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path.as_posix())
        else:
            raise ValueError(f"path is not a file or directory: {path}")
        logger.debug(
            event="Deleted object",
            path=path,
        )
        return
