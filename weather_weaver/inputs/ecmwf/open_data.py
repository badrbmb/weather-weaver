import datetime as dt
from enum import Enum
from pathlib import Path
from typing import Any

import cfgrib
import dask_geopandas as dask_gpd
import geopandas as gpd
import shapely
import structlog
import xarray as xr
from ecmwf.opendata import Client as ECMWFClient

from weather_weaver.inputs.ecmwf import constants
from weather_weaver.models.fetcher import FetcherInterface
from weather_weaver.models.processor import BaseProcessor
from weather_weaver.models.request import BaseRequest

logger = structlog.getLogger()

# ------- Request model ------- #


class StreamType(Enum):
    ENFO = "enfo"
    OPER = "oper"


class RunTime(Enum):
    H00 = 0
    H06 = 6
    H12 = 12
    H18 = 18


class RequestType(Enum):
    PERTUBED_FORECAST = "pf"  # Compatible with ENS
    FORECAST = "fc"  # Compatible with HRES


class ECMWFOpenDataRequest(BaseRequest):
    run_date: dt.date
    run_time: RunTime
    stream: StreamType
    request_type: RequestType
    nwp_parameters: list[str] = constants.NWP_PARAMETERS
    forecast_steps: list[int] = constants.FORECAST_STEPS

    @property
    def file_name(self) -> str:
        """File name based on request parameters."""
        return "_".join(
            [
                self.run_date.strftime("%Y%m%d"),
                f"{str(self.run_time.value).zfill(2)}z",
                f"{self.forecast_steps[0]}-{self.forecast_steps[-1]}",
                self.stream.value,
                self.request_type.value,
            ],
        )

    def to_ecmwf_request(self) -> dict[str, Any]:
        """Create request compatible with ECMWF."""
        return {
            "stream": self.stream.value,
            "type": self.request_type.value,
            "param": self.nwp_parameters,
            "date": self.run_date,
            "time": self.run_time.value,
            "step": self.forecast_steps,
        }

    @property
    def variables(self) -> list[str]:
        """Returns list of variables based on the request stream type."""
        id_vars = ["latitude", "longitude", "run_time", "step"]
        if self.stream == StreamType.ENFO:
            id_vars += ["number"]
        return id_vars


# ------- Fetcher model ------- #


class ECMWFOpenDataFetcher(FetcherInterface):
    def __init__(
        self,
        data_source: str = constants.DATA_SOURCE,
        data_dir: Path = constants.RAW_DIR,
    ) -> None:
        super().__init__()
        self.data_source = data_source
        self.client = ECMWFClient(source=self.data_source)
        self.data_dir = data_dir
        logger.debug(
            event="Init fetcher",
            source="ECMWF",
            data_source=data_source,
            data_dir=data_dir,
        )

    def list_raw_files(self, request: ECMWFOpenDataRequest) -> list[tuple[str, tuple[int]]] | None:
        """List all raw files matching a given request."""
        try:
            results = self.client._get_urls(
                request=request.to_ecmwf_request(),
                target=None,
                use_index=True,
            )
        except Exception as e:
            logger.error(
                event="Listing raw files failed.",
                error=e,
                request=request,
            )
            return None
        return results.urls

    def download_raw_files(
        self,
        request: ECMWFOpenDataRequest,
    ) -> Path | None:
        """Wrapper around ECMWF open data client."""
        tmp_folder = self.data_dir / request.stream.value
        tmp_folder.mkdir(exist_ok=True)

        destination_path = tmp_folder / f"{request.file_name}.grib2"

        if destination_path.exists():
            logger.debug(
                event="Download raw files skipped.",
                fetcher=self.__class__.__name__,
                data_source=self.data_source,
                request=request,
                destination_path=destination_path,
            )
            return destination_path

        try:
            self.client.retrieve(
                request=request.to_ecmwf_request(),
                target=destination_path,
            )
        except Exception as e:
            logger.error(
                event="Download raw files failed.",
                error=e,
                request=request,
            )
            return None

        logger.debug(
            event="Download raw files complete.",
            fetcher=self.__class__.__name__,
            data_source=self.data_source,
            request=request,
            destination_path=destination_path,
        )
        return destination_path


# ------- Processor model ------- #


class GeoFilterModel:
    def __init__(self, filter_df: gpd.GeoDataFrame, method: str) -> None:
        self.filter_df = filter_df
        self.method = method

    def prefilter_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """Pre-filter a dataset using longitude (the most numerous dimension)."""
        bounds = self.bounds
        return dataset.sel(longitude=slice(bounds["min_lon"], bounds["max_lon"]))

    def filter_dask(self, ddf: dask_gpd.GeoDataFrame) -> dask_gpd.GeoDataFrame:
        """Filter a dask dataframe based on the filtder_df and method."""
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


class EMCWFOpenDataProcessor(BaseProcessor):
    @staticmethod
    def load(path: Path) -> list[xr.Dataset]:
        """Load raw files."""
        return cfgrib.open_datasets(
            path=path,
            chunks={
                "time": 1,
                "step": -1,
                "longitude": "auto",
                "latitude": "auto",
            },
            backend_kwargs={"indexpath": ""},
        )

    @staticmethod
    def merge_datasets(datasets: list[xr.Dataset]) -> xr.Dataset:
        """Merge all datasets into a single one."""
        # each dataset is specific to a single param
        for i, ds in enumerate(datasets):
            # Delete unwanted coordinates
            ds = ds.drop_vars(
                names=[c for c in ds.coords if c not in constants.COORDINATE_ALLOW_LIST],
                errors="ignore",
            )
            # Put the modified dataset back in the list
            datasets[i] = ds

        # merge all datasets
        merged_dataset = xr.merge(
            objects=datasets,
            compat="override",
            combine_attrs="drop_conflicts",
        )
        # rename time field to make explicit this is the init time
        merged_dataset = merged_dataset.rename({"time": "run_time"})
        return merged_dataset

    @staticmethod
    def process(
        dataset: xr.Dataset,
        id_vars: list[str],
        normalise: bool = False,
    ) -> dask_gpd.GeoDataFrame:
        """Convert a xarray dataset to a dask GeoDataFrame."""
        ddf = dataset.to_dask_dataframe()
        if normalise:
            ddf = ddf.melt(
                id_vars=id_vars,
                var_name="variable",
                value_name="value",
            )
        # compute actual value_datetime
        ddf["timestamp"] = ddf["run_time"] + ddf["step"]

        # assign geometry
        ddf = dask_gpd.from_dask_dataframe(
            ddf,
            geometry=dask_gpd.points_from_xy(ddf, "longitude", "latitude"),
        )
        ddf = ddf.set_crs(4326)
        return ddf

    @staticmethod
    def post_process(ddf: dask_gpd.GeoDataFrame) -> dask_gpd.GeoDataFrame:
        """Post process df by dropping non-required columns."""
        ddf = ddf.drop(columns=["geometry", "index_right", "step"])
        return ddf

    def transform(
        self,
        raw_path: Path,
        request: ECMWFOpenDataRequest,
        filter_model: GeoFilterModel | None,
        normalise: bool = False,
    ) -> dask_gpd.GeoDataFrame:
        """Process raw file."""
        datasets = self.load(raw_path)
        dataset = self.merge_datasets(datasets)
        if filter_model is not None:
            dataset = filter_model.prefilter_dataset(dataset)
        ddf = self.process(
            dataset=dataset,
            id_vars=request.variables,
            normalise=normalise,
        )
        if filter_model is not None:
            ddf = filter_model.filter_dask(ddf)
        ddf = self.post_process(ddf)

        return ddf
