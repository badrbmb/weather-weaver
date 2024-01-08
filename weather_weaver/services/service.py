import datetime as dt
import time
from pathlib import Path

import dask
import dask.dataframe
import dask_geopandas as dask_gpd
import structlog

from weather_weaver.constants import MIN_VALID_SIZE_BYTES
from weather_weaver.models.fetcher import FetcherInterface
from weather_weaver.models.geotagger import GeoFilterModel
from weather_weaver.models.processor import BaseProcessor
from weather_weaver.models.request import BaseRequest, BaseRequestBuilder
from weather_weaver.models.storage import StorageInterface

dask.config.set({"array.slicing.split_large_chunks": True})
logger = structlog.getLogger()

MAX_THREADS = 4


class WeatherConsumerService:
    """Service class for the weather data consumer.

    Each method implements a use case.
    """

    def __init__(
        self,
        *,
        request_builder: BaseRequestBuilder,
        fetcher: FetcherInterface,
        raw_dir: Path,
        processor: BaseProcessor,
        storer: StorageInterface,
        processed_dir: Path,
    ) -> None:
        self.request_builder = request_builder
        self.raw_dir = raw_dir
        self.fetcher = fetcher
        self.processor = processor
        self.storer = storer
        self.processed_dir = processed_dir
        # get geofilter based on request area
        self.geo_filter = GeoFilterModel.from_iso3_list(self.request_builder.area)

    def _build_default_requests(self, start: dt.date, date_offset: int) -> list[BaseRequest]:
        all_run_dates: list[dt.datetime] = [
            start + dt.timedelta(days=i) for i in range(date_offset)
        ]
        all_requests: list[list[BaseRequest]] = [
            self.request_builder.build_default_requests(run_date=run_date)
            for run_date in all_run_dates
        ]
        # flatten and return list
        return [u for v in all_requests for u in v]

    def _download(self, request: BaseRequest) -> tuple[Path | None, BaseRequest]:
        raw_path = self.fetcher.download_raw_file(
            raw_dir=self.raw_dir,
            request=request,
        )
        return raw_path, request

    def _process(
        self,
        raw_path_request: tuple[Path, BaseRequest],
    ) -> tuple[dask_gpd.GeoDataFrame, str]:
        raw_path, request = raw_path_request
        processed = self.processor.transform(
            raw_path=raw_path,
            request=request,
            geo_filter=self.geo_filter,
        )
        return processed, request.file_name

    def _store(
        self,
        processed_file_name: tuple[dask_gpd.GeoDataFrame, str],
    ) -> Path:
        processed, file_name = processed_file_name
        return self.storer.store(
            ddf=processed,
            destination_path=self.processed_dir / f"{file_name}.parquet",
        )

    def _build_dask_pipeline(
        self,
        all_new_requests: list[BaseRequest],
        npartitions: int,
    ) -> dask.bag.Bag:
        dask_bag = (
            dask.bag.from_sequence(seq=all_new_requests, npartitions=npartitions)
            .map(self._download)
            # filter out failed downloads
            .filter(lambda raw_path_request: raw_path_request[0] is not None)
            .map(self._process)
            .map(self._store)
        )
        return dask_bag

    def _is_valid_file(self, path: Path, min_size_bytes: float = MIN_VALID_SIZE_BYTES) -> bool:
        """Helper function to check if a file is valid."""
        if self.storer.exists(path=path):
            if path.is_file():
                total_size = path.stat().st_size
            else:
                total_size = sum(f.stat().st_size for f in path.glob("**/*") if f.is_file())
            return total_size > min_size_bytes
        return False

    def download_datasets(
        self,
        start: dt.date,
        date_offset: int,
    ) -> list[Path]:
        """Download datasets for all dates between start and end."""
        start_time = time.perf_counter()
        logger.info(
            event="Download datasets: START",
            start=start,
            date_offset=date_offset,
        )
        all_requests = self._build_default_requests(start=start, date_offset=date_offset)

        # check the ones already processed
        all_new_requests = [
            t
            for t in all_requests
            if not self._is_valid_file(path=self.processed_dir / f"{t.file_name}.parquet")
        ]

        logger.debug(
            event="Built requests.",
            count_new_requests=len(all_new_requests),
            count_all_requests=len(all_requests),
        )

        # define the pipeline
        pipeline = (
            self._build_dask_pipeline(
                all_new_requests,
                npartitions=len(all_new_requests),
            )
            if len(all_new_requests) > 0
            else None
        )
        # run the pipeline
        processed_files: list[Path] = pipeline.compute() if pipeline is not None else []

        end_time = time.perf_counter()
        logger.info(
            event="Download datasets: END",
            count_processed_files=len(processed_files),
            count_failed_files=len(all_new_requests) - len(processed_files),
            elapsed_time_secs=end_time - start_time,
        )

        return processed_files
