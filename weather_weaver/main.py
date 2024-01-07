import datetime as dt
from enum import Enum

import structlog
import typer
from dask.distributed import Client
from typing_extensions import Annotated

from weather_weaver import constants
from weather_weaver.inputs.ecmwf import constants as ecmwf_constants
from weather_weaver.inputs.ecmwf.open_data.fetcher import ECMWFOpenDataFetcher
from weather_weaver.inputs.ecmwf.open_data.processor import EMCWFOpenDataProcessor
from weather_weaver.inputs.ecmwf.open_data.request import ECMWFOpenDataRequestBuilder
from weather_weaver.outputs.localfs.client import LocalClient
from weather_weaver.services.service import WeatherConsumerService

logger = structlog.getLogger()
app = typer.Typer()

DEFAULT_DATE_FORMAT = "%Y-%m-%d"

DEFAULT_WORKER_NUMBER = 1


class DataSource(str, Enum):
    EMCWF_OPEN_DATA = "EMCWF [Open data]"
    EMCWF_ARCHIVE = "EMCWF [Archive (2017-2020)]"


class StorageLocation(str, Enum):
    LOCAL_FS = "local"
    AWS_S3 = "s3"


class Area(str, Enum):
    ENTSOE = "entsoe"


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
    area: Annotated[
        Area,
        typer.Option(help="the geographical area to download data for."),
    ] = Area.ENTSOE,
    storage: Annotated[
        StorageLocation,
        typer.Option(help="the location where to store processed files."),
    ] = StorageLocation.LOCAL_FS,
    date_offset: Annotated[
        int,
        typer.Option(help="the number of days to download data for, starting from the start date."),
    ] = 1,
    n_workers: Annotated[
        int,
        typer.Option(help="the number of workers in dask cluster."),
    ] = DEFAULT_WORKER_NUMBER,
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

    match area:
        case Area.ENTSOE:
            country_iso3s = constants.ENTSO_E_ISO3_LIST
        case _:
            raise NotImplementedError(f"{area=} not implemented yet!")

    match source:
        case DataSource.EMCWF_OPEN_DATA:
            fetcher = ECMWFOpenDataFetcher()
            request_builder = ECMWFOpenDataRequestBuilder(area=country_iso3s)
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
    dask_client = Client(
        name="Weather Weaver data download.",
        n_workers=n_workers,
    )

    logger.info(
        event="Dask cluster started.",
        url=dask_client.dashboard_link,
        n_workers=n_workers,
    )

    try:
        _ = service.download_datasets(start=start, date_offset=date_offset)
    except Exception as e:
        raise e
    finally:
        # close the dask client
        dask_client.close()
        logger.debug(
            event="Dask cluster closed.",
        )


if __name__ == "__main__":
    app()
