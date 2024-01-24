import shutil
from pathlib import Path

import dask.dataframe
import structlog

from weather_weaver.constants import MIN_VALID_SIZE_BYTES
from weather_weaver.models.storage import StorageInterface

logger = structlog.getLogger()


class LocalClient(StorageInterface):
    def exists(self, *, path: Path) -> bool:
        """Check if file exists."""
        return path.exists()

    def is_valid(self, *, path: Path, min_size_bytes: float = MIN_VALID_SIZE_BYTES) -> bool:
        """Check if a file is valid."""
        if self.exists(path=path):
            if path.is_file():
                total_size = path.stat().st_size
            else:
                total_size = sum(f.stat().st_size for f in path.glob("**/*") if f.is_file())
            return total_size > min_size_bytes
        return False

    def list_files_for_request(self, *, folder: Path, extension: str = "parquet") -> list[Path]:
        """List the available files."""
        return folder.glob(f"*.{extension}")

    def store(self, *, ddf: dask.dataframe.DataFrame, destination_path: Path) -> Path:
        """Store a dataset."""
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        ddf.to_parquet(destination_path, write_index=False)
        logger.debug(
            event="Stored DataFrame",
            destination_path=destination_path,
        )
        return destination_path

    def delete(self, *, path: Path) -> None:
        """Delete object by path."""
        if not path.exists():
            raise FileNotFoundError(f"file does not exist: {path}")
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path.as_posix())
        else:
            raise ValueError(f"path is not a file or directory: {path}")
        logger.debug(
            event="Deleted path",
            path=path,
        )
        return
