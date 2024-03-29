from pathlib import Path
from typing import Protocol

import dask.dataframe

from weather_weaver.models.request import BaseRequest


class StorageInterface(Protocol):
    """Generic interface for storing fetched NWP data."""

    def exists(self, *, path: Path) -> bool:
        """Check if file exists in store."""
        pass

    def is_valid(self, *, path: Path, min_size_bytes: float) -> bool:
        """Check if a file is valid."""

    def list_files_for_request(self, *, requests: list[BaseRequest]) -> list[Path]:
        """List the available files for requests."""
        pass

    def store(self, *, ddf: dask.dataframe.DataFrame, destination_path: Path) -> Path:
        """Store a dataset."""
        pass

    def delete(self, *, path: Path) -> bool:
        """Delete a given record using path."""
        pass
