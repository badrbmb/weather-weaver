import shutil
from pathlib import Path

import geopandas as gpd
import requests

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
