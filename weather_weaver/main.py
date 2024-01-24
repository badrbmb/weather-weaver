import datetime as dt
from enum import Enum

import structlog
import typer
from dask.distributed import Client
from distributed import SpecCluster
from typing_extensions import Annotated

from weather_weaver import constants
from weather_weaver.inputs.ecmwf import constants as ecmwf_constants
from weather_weaver.models.fetcher import FetcherInterface
from weather_weaver.models.geo import GeoFilterModel
from weather_weaver.models.processor import BaseProcessor
from weather_weaver.models.request import BaseRequestBuilder
from weather_weaver.models.storage import StorageInterface
from weather_weaver.outputs.localfs.client import LocalClient
from weather_weaver.services.service import WeatherConsumerService
from weather_weaver.utils import OffsetFrequency

logger = structlog.getLogger()
app = typer.Typer()

DEFAULT_DATE_FORMAT = "%Y-%m-%d"

DEFAULT_WORKER_NUMBER = 1
DEFAULT_THREADS_BY_WORKER = 5


class DataSource(str, Enum):
    ECMWF_OPEN_DATA = "ECMWF [Open data]"
    ECMWF_CDS_ERA5 = "ECMWF ERA5"


class StorageLocation(str, Enum):
    LOCAL_FS = "local"
    AWS_S3 = "s3"


class Area(str, Enum):
    ENTSOE = "entsoe"


class ClusterType(str, Enum):
    LOCAL = "local"


def load_area(area: Area) -> GeoFilterModel:
    """Load geofilter matching the specified area."""
    match area:
        case Area.ENTSOE:
            geo_filter = GeoFilterModel.from_bounding_box(
                constants.EUROPE_BOUNDING_BOX_STR,
            ).filter_iso3s(constants.ENTSO_E_ISO3_LIST)

        case _:
            raise NotImplementedError(f"{area=} not implemented yet!")
    return geo_filter


def load_source(
    source: DataSource,
    geo_filter: GeoFilterModel,
) -> tuple[FetcherInterface, BaseRequestBuilder, BaseProcessor]:
    """Load fetcher, builder and processor for a given data source."""
    match source:
        case DataSource.ECMWF_OPEN_DATA:
            from weather_weaver.inputs.ecmwf.open_data.fetcher import ECMWFOpenDataFetcher
            from weather_weaver.inputs.ecmwf.open_data.request import ECMWFOpenDataRequestBuilder
            from weather_weaver.inputs.ecmwf.processor import EMCWFProcessor

            fetcher = ECMWFOpenDataFetcher()
            request_builder = ECMWFOpenDataRequestBuilder(geo_filter=geo_filter)
            processor = EMCWFProcessor()
        case DataSource.ECMWF_CDS_ERA5:
            from weather_weaver.inputs.ecmwf.cds.fetcher import ECMWFCDSFetcher
            from weather_weaver.inputs.ecmwf.cds.request import ECMWFCDSRequestBuilder
            from weather_weaver.inputs.ecmwf.processor import EMCWFProcessor

            fetcher = ECMWFCDSFetcher()
            request_builder = ECMWFCDSRequestBuilder(geo_filter=geo_filter)
            processor = EMCWFProcessor()
        case _:
            raise NotImplementedError(f"{source=} not implemented yet!")

    return fetcher, request_builder, processor


def load_storage(storage: StorageLocation) -> StorageInterface:
    """Load storer."""
    match storage:
        case StorageLocation.LOCAL_FS:
            storer = LocalClient()
        case _:
            raise NotImplementedError(f"{storage=} not implemented yet!")

    return storer


def load_cluster(
    cluster_type: ClusterType,
    n_workers: int,
    threads_per_worker: int,
    name: str = "Weather Weaver data download cluster.",
) -> SpecCluster:
    """Load cluster."""
    match cluster_type:
        case ClusterType.LOCAL:
            from distributed import LocalCluster

            cluster = LocalCluster(
                name=name,
                n_workers=n_workers,
                threads_per_worker=threads_per_worker,
            )
        case _:
            raise NotImplementedError(f"{cluster_type=} not implemented yet!")

    return cluster


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
    offset_frequency: Annotated[
        OffsetFrequency,
        typer.Option(help="the frequency of the date_offset."),
    ] = OffsetFrequency.DAILY,
    cluster_type: Annotated[
        ClusterType,
        typer.Option(help="the type of cluster."),
    ] = ClusterType.LOCAL,
    n_workers: Annotated[
        int,
        typer.Option(help="the number of workers in dask cluster."),
    ] = DEFAULT_WORKER_NUMBER,
    threads_per_worker: Annotated[
        int,
        typer.Option(help="the number of threads per worker in the cluster."),
    ] = DEFAULT_THREADS_BY_WORKER,
) -> None:
    """CLI method to download datasets."""
    # parse date str
    _start = (
        dt.datetime.strptime(
            start,
            DEFAULT_DATE_FORMAT,
        )
        .astimezone(dt.timezone.utc)
        .date()
    )

    # load all ressources
    geo_filter = load_area(area)

    fetcher, request_builder, processor = load_source(
        source=source,
        geo_filter=geo_filter,
    )

    storer = load_storage(storage=storage)

    service = WeatherConsumerService(
        request_builder=request_builder,
        raw_dir=ecmwf_constants.RAW_DIR,
        processed_dir=ecmwf_constants.PROCESSED_DIR,
        fetcher=fetcher,
        processor=processor,
        storer=storer,
    )

    cluster = load_cluster(
        cluster_type=cluster_type,
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
    )

    # start a local dask cluster
    dask_client = Client(cluster)

    logger.info(
        event="Dask cluster started.",
        url=dask_client.dashboard_link,
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
    )

    try:
        _ = service.download_datasets(
            start=_start,
            date_offset=date_offset,
            offset_frequency=offset_frequency,
            # scheduler option: "threads", "multiprocessing" or "synchronous"
            # to override default.
            # more info: https://docs.dask.org/en/stable/scheduling.html
            scheduler=None,
        )
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
