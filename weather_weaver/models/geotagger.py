import shutil
from pathlib import Path

import dask_geopandas as dask_gpd
import geopandas as gpd
import requests
import shapely
import xarray as xr

from weather_weaver.config import DATA_DIR


def download_world_countries(output_path: Path, resolution: str = "110m") -> None:
    """Download file with countries' boundaries from NaturalEarth data."""
    url = f"https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/{resolution}/cultural/ne_{resolution}_admin_0_countries.zip"
    # download the file from this url to a tmp location @ tmp_path
    response = requests.get(url=url, stream=True, timeout=20)
    if response.status_code == 200:
        shutil.copyfileobj(response.raw, output_path)
    else:
        raise ValueError(f"Failed downloading countries boundaries to {output_path}")


def load_world_countries(resolution: str = "110m") -> gpd.GeoDataFrame:
    """Loads a geodataframe with all country names and geometrie."""
    path = DATA_DIR / f"ne_{resolution}_admin_0_countries.zip"
    if not path.exists():
        # download file
        download_world_countries(output_path=path, resolution=resolution)
    world = gpd.read_file(path)
    world.rename(
        columns={
            "ADM0_ISO": "country_iso3",
            "NAME": "country_name",
        },
        inplace=True,
    )
    return world[["country_name", "country_iso3", "geometry"]].copy()


class GeoFilterModel:
    def __init__(self, *, filter_df: gpd.GeoDataFrame, method: str) -> None:
        self.filter_df = filter_df
        self.method = method

    def prefilter_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """Pre-filter a dataset using longitude (the most numerous dimension)."""
        bounds = self.bounds
        return dataset.sel(longitude=slice(bounds["min_lon"], bounds["max_lon"]))

    def filter_dask(self, ddf: dask_gpd.GeoDataFrame) -> dask_gpd.GeoDataFrame:
        """Filter a dask dataframe based on the fitlder_df and method.

        Also used as a geotagging by asssigning the matching iso3 to each row in ddf.
        """
        return ddf.sjoin(self.filter_df, predicate=self.method)

    @property
    def bounds(self) -> dict[str, float]:
        """Boundaries of the all the geometries in filter_df."""
        min_lon, min_lat, max_lon, max_lat = shapely.unary_union(self.filter_df.geometry).bounds
        return {
            "min_lon": min_lon,
            "min_lat": min_lat,
            "max_lon": max_lon,
            "max_lat": max_lat,
        }

    @classmethod
    def from_iso3_list(cls, list_iso3s: list[str]) -> "GeoFilterModel":  # noqa: ANN102
        """Create an instance of GeoFilterModel using a list of iso3s."""
        world = load_world_countries()
        return cls(
            filter_df=world[world["country_iso3"].isin(list_iso3s)],
            method="within",
        )
