from pathlib import Path
from typing import Protocol

from weather_weaver.models.request import BaseRequest


class FetcherInterface(Protocol):
    """Generic interface for fetching and converting NWP data from an external sources."""

    def list_raw_files(self, *, request: BaseRequest) -> list:
        """List the relative path of all files available from source for the given run_date."""
        pass

    def download_raw_files(self, *, request: BaseRequest) -> Path:
        """Fetch the bytes of a single raw file from source and save to local file."""
        pass
