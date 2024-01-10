from pathlib import Path

import cdsapi
import structlog

from weather_weaver.constants import MIN_VALID_SIZE_BYTES
from weather_weaver.inputs.ecmwf.cds.request import ECMWFCDSRequest
from weather_weaver.models.fetcher import FetcherInterface

logger = structlog.getLogger()


class ECMWFCDSFetcher(FetcherInterface):
    def __init__(
        self,
    ) -> None:
        super().__init__()
        self.client = cdsapi.Client()
        logger.debug(
            event="Init fetcher",
            source="CDS",
        )

    def list_raw_files(self) -> None:
        """List all raw files matching a given request."""
        raise NotImplementedError

    def download_raw_file(
        self,
        request: ECMWFCDSRequest,
        raw_dir: Path,
        update: bool = False,
        min_size_bytes: float = MIN_VALID_SIZE_BYTES,
    ) -> Path:
        """Wrapper around Copernicus CDS client."""
        destination_path = raw_dir / f"{request.file_name}.grib"
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        if (
            destination_path.exists()
            and destination_path.stat().st_size > min_size_bytes
            and not update
        ):
            logger.debug(
                event="Download raw files skipped.",
                fetcher=self.__class__.__name__,
                years=request.years,
                product_type=request.product_type,
                destination_path=destination_path,
            )
            return destination_path

        try:
            logger.debug(
                event="Download raw files starting, check status @url",
                url="https://cds.climate.copernicus.eu/cdsapp#!/yourrequests",
            )
            self.client.retrieve(
                name=request.dataset.value,
                request=request.to_cds_request(),
                target=destination_path,
            )
        except Exception as e:
            logger.error(
                event="Download raw files failed.",
                error=e,
                request=request,
            )
            # delete partial download file
            if destination_path.is_file():
                destination_path.unlink()
            return None

        logger.debug(
            event="Download raw files complete.",
            fetcher=self.__class__.__name__,
            years=request.years,
            product_type=request.product_type,
            destination_path=destination_path,
        )
        return destination_path
