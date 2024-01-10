from abc import ABC, abstractmethod
from pathlib import Path

import dask_geopandas as dask_gpd

from weather_weaver.models.geo import GeoFilterModel


class BaseProcessor(ABC):
    @abstractmethod
    def transform(
        self,
        *,
        raw_path: Path,
        geo_filter: GeoFilterModel | None = None,
    ) -> dask_gpd.GeoDataFrame:
        """Process a raw file given a request.

        Optionally tags the countries using a GeoFilterModel.
        """
        pass
