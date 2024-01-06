from pathlib import Path

import cfgrib
import dask_geopandas as dask_gpd
import xarray as xr

from weather_weaver.inputs.ecmwf import constants
from weather_weaver.inputs.ecmwf.open_data.request import ECMWFOpenDataRequest
from weather_weaver.models.geotagger import GeoFilterModel
from weather_weaver.models.processor import BaseProcessor


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
        """Post process df by dropping non-required columns if they exist."""
        columns_to_drop = ["geometry", "index_right", "step"]
        existing_columns_to_drop = [col for col in columns_to_drop if col in ddf.columns]
        ddf = ddf.drop(columns=existing_columns_to_drop)
        return ddf

    def transform(
        self,
        raw_path: Path,
        request: ECMWFOpenDataRequest,
        geo_filter: GeoFilterModel | None = None,
    ) -> dask_gpd.GeoDataFrame:
        """Process raw file."""
        datasets = self.load(raw_path)
        dataset = self.merge_datasets(datasets)
        if geo_filter is not None:
            dataset = geo_filter.prefilter_dataset(dataset)
        ddf = self.process(
            dataset=dataset,
            id_vars=request.variables,
            normalise=request.normalise_data,
        )
        if geo_filter is not None:
            ddf = geo_filter.filter_dask(ddf)
        ddf = self.post_process(ddf)

        return ddf
