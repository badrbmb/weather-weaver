import datetime as dt
from enum import Enum

import structlog
import typer
from dask.distributed import Client
from typing_extensions import Annotated

from weather_weaver.inputs.ecmwf import constants as ecmwf_constants
from weather_weaver.inputs.ecmwf.open_data.fetcher import ECMWFOpenDataFetcher
from weather_weaver.inputs.ecmwf.open_data.processor import EMCWFOpenDataProcessor
from weather_weaver.inputs.ecmwf.open_data.request import ECMWFOpenDataRequestBuilder
from weather_weaver.outputs.localfs.client import LocalClient
from weather_weaver.services.service import WeatherConsumerService

logger = structlog.getLogger()
app = typer.Typer()

DEFAULT_DATE_FORMAT = "%Y-%m-%d"


class DataSource(str, Enum):
    EMCWF_OPEN_DATA = "EMCWF [Open data]"
    EMCWF_ARCHIVE = "EMCWF [Archive (2017-2020)]"


class StorageLocation(str, Enum):
    LOCAL_FS = "local"
    AWS_S3 = "s3"


@app.command()
def download_datasets(
    start: Annotated[
        str,
        typer.Argument(
            ...,
            formats=[DEFAULT_DATE_FORMAT],
            help="the start date for data downloading.",
        ),
    ],
    source: Annotated[
        DataSource,
        typer.Option(..., help="the source to download NPW data from."),
    ],
    storage: Annotated[
        StorageLocation,
        typer.Option(help="the location where to store processed files."),
    ] = StorageLocation.LOCAL_FS,
    date_offset: Annotated[
        int,
        typer.Option(help="the number of days to download data for, starting from the start date."),
    ] = 1,
) -> None:
    """CLI method to download datasets."""
    # parse date str
    start = (
        dt.datetime.strptime(
            start,
            DEFAULT_DATE_FORMAT,
        )
        .astimezone(dt.timezone.utc)
        .date()
    )

    match source:
        case DataSource.EMCWF_OPEN_DATA:
            fetcher = ECMWFOpenDataFetcher()
            request_builder = ECMWFOpenDataRequestBuilder()
            processor = EMCWFOpenDataProcessor()
        case _:
            raise NotImplementedError(f"{source=} not implemented yet!")

    match storage:
        case StorageLocation.LOCAL_FS:
            storer = LocalClient()
        case _:
            raise NotImplementedError(f"{storage=} not implemented yet!")

    service = WeatherConsumerService(
        request_builder=request_builder,
        raw_dir=ecmwf_constants.RAW_DIR,
        processed_dir=ecmwf_constants.PROCESSED_DIR,
        fetcher=fetcher,
        processor=processor,
        storer=storer,
    )

    # start a local dask cluster
    dask_client = Client()

    logger.info(
        event="Dask cluster started",
        url=dask_client.dashboard_link,
    )

    _ = service.download_datasets(start=start, date_offset=date_offset)

    # close the dask client
    dask_client.close()

    logger.info(
        event="Dask cluster closed",
        url=dask_client.dashboard_link,
    )


if __name__ == "__main__":
    app()
