import datetime as dt
import hashlib
from enum import Enum
from typing import Any, Generator

from weather_weaver.inputs.ecmwf.cds import constants
from weather_weaver.models.geo import BoundingBox
from weather_weaver.models.request import BaseRequest, BaseRequestBuilder


class ProductType(Enum):
    REANALYSIS = "reanalysis"


class DatasetName(Enum):
    ERA5_SINGLE_LEVELS = "reanalysis-era5-single-levels"


class ECMWFCDSRequest(BaseRequest):
    dataset: DatasetName
    years: list[str]
    months: list[str]
    days: list[str]
    times: list[str]
    nwp_parameters: list[str]
    product_type: ProductType
    area: BoundingBox

    @property
    def file_name(self) -> str:
        """File name based on request parameters."""
        params_json = self.model_dump_json()
        return f"{self.dataset.value}/{hashlib.sha256(params_json.encode("utf-8")).hexdigest()}"

    def to_cds_request(self) -> dict[str, Any]:
        """Create request compatible with CDS client."""
        latlon_bounds = self.bounding_box.to_latlon_dict()
        return {
            "product_type": self.product_type.value,
            "variable": self.nwp_parameters,
            "year": self.years,
            "month": self.months,
            "day": self.days,
            "time": self.times,
            "area": [
                latlon_bounds["max_lat"],
                latlon_bounds["min_lon"],
                latlon_bounds["min_lat"],
                latlon_bounds["max_lon"],
            ],
            "format": "grib",
        }

    @property
    def variables(self) -> list[str]:
        """Returns list of variables based on the request stream type."""
        return ["latitude", "longitude", "run_time", "step"]


class ECMWFCDSRequestBuilder(BaseRequestBuilder):
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN003, ANN002
        super().__init__(*args, **kwargs)
        self.default_nwp_parameters = constants.NWP_PARAMETERS

    def build_default_requests(
        self,
        run_date: dt.date,
    ) -> list[ECMWFCDSRequest]:
        """Return all the default requests for the **whole year** of the run_date (historical data).

        Download default variables as defined in constants.NWP_PARAMETERS.
        covering all months and days and times in the year.
        """
        return [
            ECMWFCDSRequest(
                dataset=DatasetName.ERA5_SINGLE_LEVELS,
                years=[str(run_date.year)],
                months=constants.DEFAULT_MONTHS,
                days=constants.DEFAULT_DAYS,
                times=constants.DEFAULT_TIMES,
                product_type=ProductType.REANALYSIS,
                area=self.geo_filter.bounding_box,
            ),
        ]

    def build_closest_requests(
        self,
        target_run_date: dt.datetime,
    ) -> Generator[BaseRequest, None, None]:
        """Builds the request closest to the desired target_run_date."""
        raise NotImplementedError()
