from pathlib import Path

import structlog
from ecmwf.opendata import Client as ECMWFClient

from weather_weaver.inputs.ecmwf import constants
from weather_weaver.inputs.ecmwf.open_data.request import ECMWFOpenDataRequest
from weather_weaver.models.fetcher import FetcherInterface

logger = structlog.getLogger()


class ECMWFOpenDataFetcher(FetcherInterface):
    def __init__(
        self,
        data_source: str = constants.DATA_SOURCE,
    ) -> None:
        super().__init__()
        self.data_source = data_source
        self.client = ECMWFClient(source=self.data_source)
        logger.debug(
            event="Init fetcher",
            source="ECMWF",
            data_source=data_source,
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

    def download_raw_file(
        self,
        request: ECMWFOpenDataRequest,
        raw_dir: Path,
        update: bool = False,
    ) -> Path | None:
        """Wrapper around ECMWF open data client."""
        destination_path = raw_dir / f"{request.file_name}.grib2"
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        if destination_path.exists() and not update:
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
