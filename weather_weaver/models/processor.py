from abc import ABC, abstractmethod
from pathlib import Path

import dask_geopandas as dask_gpd

from weather_weaver.models.request import BaseRequest


class BaseProcessor(ABC):
    @abstractmethod
    def transform(self, *, raw_path: Path, request: BaseRequest) -> dask_gpd.GeoDataFrame:
        """Process a raw file given a request."""
        pass
