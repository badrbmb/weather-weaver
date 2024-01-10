import re
import shutil
from functools import lru_cache
from pathlib import Path

import dask_geopandas as dask_gpd
import geopandas as gpd
import requests
import shapely
import xarray as xr

from weather_weaver.config import DATA_DIR


class BoundingBox:
    def __init__(self, north: float, west: float, south: float, east: float) -> None:
        self.north = north
        self.south = south
        self.west = west
        self.east = east

    @property
    def geometry(self) -> shapely.Polygon:
        """Returns box defined by bounds."""
        return shapely.box(self.west, self.south, self.east, self.north)

    @classmethod
    def from_str(cls, value: str) -> "BoundingBox":  # noqa: ANN102
        """Parse string to create a BoundingBox.

        Example of expected format: "N: 73.5 W: -27 S: 33 E: 45"
        """
        coords = map(float, re.findall(r"[NSWE]: ([-\d.]+)", value))
        north, west, south, east = coords
        return cls(north, west, south, east)

    def to_latlon_dict(self) -> dict[str, float]:
        """Explicitly convert bounds to lat/lon dictionary."""
        return {
            "min_lon": self.west,
            "min_lat": self.south,
            "max_lon": self.east,
            "max_lat": self.north,
        }


def download_world_countries(output_path: Path, resolution: str = "110m") -> None:
    """Download file with countries' boundaries from NaturalEarth data."""
    url = f"https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/{resolution}/cultural/ne_{resolution}_admin_0_countries.zip"
    # download the file from this url to a tmp location @ tmp_path
    response = requests.get(url=url, stream=True, timeout=20)
    if response.status_code == 200:
        shutil.copyfileobj(response.raw, output_path)
    else:
        raise ValueError(f"Failed downloading countries boundaries to {output_path}")


@lru_cache(maxsize=128)
def load_world_countries(resolution: str = "110m") -> gpd.GeoDataFrame:
    """Loads a geodataframe with all country names and geometrie."""
    path = DATA_DIR / f"ne_{resolution}_admin_0_countries.zip"
    if not path.exists():
        # download file
        download_world_countries(output_path=path, resolution=resolution)
    world = gpd.read_file(path)
    world.rename(
        columns={
            "ADM0_A3": "country_iso3",
            "NAME": "country_name",
        },
        inplace=True,
    )
    return world[["country_name", "country_iso3", "geometry"]].copy()


class GeoFilterModel:
    def __init__(self, *, filter_df: gpd.GeoDataFrame, method: str) -> None:
        self.filter_df = filter_df
        self.method = method

    def filter_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """Filter a dataset using longitude (the most numerous dimension)."""
        bounds = self.bounds
        return dataset.sel(longitude=slice(bounds["min_lon"], bounds["max_lon"]))

    def filter_dask(self, ddf: dask_gpd.GeoDataFrame) -> dask_gpd.GeoDataFrame:
        """Filter a dask dataframe based on the filter_df and method.

        Also used as a geotagging by assigning the matching iso3 to each row in ddf.
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

    @property
    def bounding_box(self) -> BoundingBox:
        """Returns BoundingBox from geometry bounds."""
        bounds = self.bounds
        return BoundingBox(
            north=bounds["max_lat"],
            west=bounds["min_lon"],
            south=bounds["min_lat"],
            east=bounds["max_lon"],
        )

    def filter_iso3s(self, list_iso3s: list[str]) -> "GeoFilterModel":
        """Create an instance of GeoFilterModel using a list of iso3s."""
        return GeoFilterModel(
            filter_df=self.filter_df[self.filter_df["country_iso3"].isin(list_iso3s)],
            method="within",
        )

    @classmethod
    def from_bounding_box(cls, bounding_box: BoundingBox | str) -> "GeoFilterModel":  # noqa: ANN102
        """Load a GeoFilterModel with filter_df limited to the bounding box specified."""
        # 1. filter the df to keep only df.geometry intersecting / within the bounding box
        # 2. clip all the geoemtries to only keep the part withing the bounding box.
        world = load_world_countries()
        # Create a Shapely Polygon from the bounding box
        if isinstance(bounding_box, str):
            bounding_box = BoundingBox.from_str(bounding_box)
        bbox_polygon = bounding_box.geometry
        # Use the polygon to filter and clip the geometries
        filtered_df = world[world.intersects(bbox_polygon)]
        clipped_df = gpd.clip(filtered_df, bbox_polygon)
        return cls(
            filter_df=clipped_df,
            method="within",
        )
