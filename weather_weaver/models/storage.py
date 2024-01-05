from pathlib import Path
from typing import Protocol

import dask.dataframe

from weather_weaver.models.request import BaseRequest


class StorageInterface(Protocol):
    """Generic interface for storing fetched NWP data."""

    def list_files_for_request(self, *, requests: list[BaseRequest]) -> None:
        """List the available files for requests."""
        pass

    def store(self, *, ddf: dask.dataframe.DataFrame, destination_path: Path) -> bool:
        """Store a dataset."""
        pass

    def delete(self, *, path: Path) -> bool:
        """Delete a given record using path."""
        pass
