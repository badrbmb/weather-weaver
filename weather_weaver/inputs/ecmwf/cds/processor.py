from pathlib import Path

import cfgrib
import dask.array as da
import dask_geopandas as dask_gpd
import xarray as xr

from weather_weaver.inputs.ecmwf.cds import constants
from weather_weaver.inputs.ecmwf.processor import EMCWFProcessor
from weather_weaver.models.geo import GeoFilterModel


class EMCWFCDSProcessor(EMCWFProcessor):
    @staticmethod
    def load(path: Path) -> list[xr.Dataset]:
        """Load raw files."""
        return cfgrib.open_datasets(path=path)

    @staticmethod
    def pre_process(datasets: list[xr.Dataset]) -> list[xr.Dataset]:
        """Pre-process datasets to help with merging them."""
        for i, ds in enumerate(datasets):
            if "time" in ds.dims and "step" in ds.dims:
                # need to reshape the dimensions to drop time/step and keep valid_time only
                valid_time_1d = da.from_array(ds["valid_time"].values, chunks=(-1)).flatten()
                ds = ds.assign_coords(valid_time=valid_time_1d)

                # Flatten the other variables and assign them to the new valid_time coordinate
                for var in ds.data_vars:
                    if "time" in ds[var].dims and "step" in ds[var].dims:
                        shape = ds[var].shape
                        new_shape = (shape[0] * shape[1],) + shape[2:]
                        reshaped_data = da.from_array(
                            ds[var].values,
                            chunks=shape,
                        ).reshape(new_shape)
                        new_dims = ("valid_time",) + ds[var].dims[2:]
                        ds[var] = (new_dims, reshaped_data)
                ds = ds.drop_vars(["time", "step"])
            elif "valid_time" not in ds.dims:
                ds = ds.swap_dims({"time": "valid_time"})

            # Delete unwanted coordinates
            ds = ds.drop_vars(
                names=[c for c in ds.coords if c not in constants.COORDINATE_ALLOW_LIST],
                errors="ignore",
            )
            # finally rename
            ds = ds.rename({"valid_time": "timestamp"})
            datasets[i] = ds

        return datasets

    @staticmethod
    def process(
        dataset: xr.Dataset,
    ) -> dask_gpd.GeoDataFrame:
        """Convert a xarray dataset to a dask GeoDataFrame."""
        ddf = dataset.to_dask_dataframe()
        # assign geometry
        ddf = dask_gpd.from_dask_dataframe(
            ddf,
            geometry=dask_gpd.points_from_xy(
                ddf,
                "longitude",
                "latitude",
                # crs=4326, # TODO: investigate why param is not set on ddf using points_from_xy
            ),
        )
        ddf.crs = 4326
        return ddf

    @staticmethod
    def post_process(ddf: dask_gpd.GeoDataFrame) -> dask_gpd.GeoDataFrame:
        """Post process df by dropping non-required columns if they exist."""
        columns_to_drop = ["geometry", "index_right"]
        existing_columns_to_drop = [col for col in columns_to_drop if col in ddf.columns]
        ddf = ddf.drop(columns=existing_columns_to_drop)

        # drop null values on parameters.
        var_parms = [
            col
            for col in ddf.columns
            if col
            not in [
                "latitude",
                "longitude",
                "timestamp",
                "country_name",
                "country_iso3",
            ]
        ]
        ddf = ddf.dropna(subset=var_parms)

        # add back run-time column
        ddf["run_time"] = ddf["timestamp"]

        return ddf

    def transform(
        self,
        raw_path: Path,
        geo_filter: GeoFilterModel,
        interpolate: bool = False,
    ) -> dask_gpd.GeoDataFrame:
        """Process raw file."""
        datasets = self.load(raw_path)
        datasets = self.pre_process(datasets)
        dataset = self.merge_datasets(datasets)
        if interpolate:
            dataset = self.interpolate(dataset)
        ddf = self.process(
            dataset=dataset,
        )

        del datasets

        ddf = geo_filter.filter_dask(ddf)
        ddf = self.post_process(ddf)

        return ddf
