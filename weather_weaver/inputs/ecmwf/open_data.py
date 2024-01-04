import datetime as dt
from enum import Enum
from pathlib import Path
from typing import Any

import structlog
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


class EMCWFOpenDataProcessor(BaseProcessor):
    def process(self, *, raw_path: Path) -> None:
        """Process raw file."""
        ...
