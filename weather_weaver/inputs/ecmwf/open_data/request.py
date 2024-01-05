import datetime as dt
from enum import Enum
from typing import Any, Generator

from weather_weaver.inputs.ecmwf import constants
from weather_weaver.models.request import BaseRequest, BaseRequestBuilder
from weather_weaver.utils import GeoFilterModel, load_world_countries


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
    nwp_parameters: list[str]
    forecast_steps: list[int]
    update_raw: bool = False
    geo_filter: GeoFilterModel | None = None
    normalise_data: bool = False

    class Config:  # noqa: D106
        arbitrary_types_allowed = True

    @property
    def file_name(self) -> str:
        """File name based on request parameters."""
        _name = "_".join(
            [
                self.run_date.strftime("%Y%m%d"),
                f"{str(self.run_time.value).zfill(2)}z",
                f"{self.forecast_steps[0]}-{self.forecast_steps[-1]}",
                self.request_type.value,
            ],
        )
        return f"{self.stream.value}/{_name}"

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


class ECMWFOpenDataRequestBuilder(BaseRequestBuilder):
    def __init__(self) -> None:
        super().__init__()
        world = load_world_countries()
        self.default_iso3s = constants.ENTSO_E_ISO3_LIST
        self.default_nwp_parameters = constants.NWP_PARAMETERS
        self.default_forecast_steps = constants.FORECAST_STEPS
        self.geo_filter = GeoFilterModel(
            filter_df=world[world["country_iso3"].isin(self.default_iso3s)],
            method="within",
        )

    def build_default_requests(
        self,
        run_date: dt.date,
    ) -> list[ECMWFOpenDataRequest]:
        """Return all the default requests for a given run_date.

        Download default variables as defined in constants.NWP_PARAMETERS,
        for all forecasting steps as defined in constants.FORECAST_STEPS,
        covering the following stream / request types / run_times
        - oper + fc @ all run times
        - ens + pfc @ all run times
        """
        all_requests = []
        for stream, request_type in zip(
            [
                StreamType.OPER,
                # StreamType.ENFO,
            ],
            [
                RequestType.FORECAST,
                # RequestType.PERTUBED_FORECAST,
            ],
        ):
            for run_time in list(RunTime):
                all_requests.append(
                    ECMWFOpenDataRequest(
                        run_date=run_date,
                        run_time=run_time,
                        stream=stream,
                        request_type=request_type,
                        nwp_parameters=self.default_nwp_parameters,
                        forecast_steps=self.default_forecast_steps,
                        geo_filter=self.geo_filter,
                    ),
                )
        return all_requests

    def build_closest_requests(
        self,
        target_run_date: dt.datetime,
    ) -> Generator[BaseRequest, None, None]:
        """Builds the request closest to the desired target_run_date."""
        raise NotImplementedError()