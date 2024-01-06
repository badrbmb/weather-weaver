import datetime as dt
import time
from pathlib import Path

import dask
import dask.dataframe
import structlog

from weather_weaver.models.fetcher import FetcherInterface
from weather_weaver.models.geotagger import GeoFilterModel
from weather_weaver.models.processor import BaseProcessor
from weather_weaver.models.request import BaseRequest, BaseRequestBuilder
from weather_weaver.models.storage import StorageInterface

dask.config.set({"array.slicing.split_large_chunks": True})
logger = structlog.getLogger()

DEFAULT_NPARTITIONS = dask.system.CPU_COUNT // 2 + 1

MIN_VALID_SIZE_BYTES = 1e6


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

    def _download_raw_file(self, requests: list[BaseRequest]) -> tuple[Path, BaseRequest]:
        for request in requests:
            yield (
                self.fetcher.download_raw_file(
                    request=request,
                    raw_dir=self.raw_dir,
                ),
                request,
            )

    def _transform(
        self,
        raw_path_request: tuple[Path, BaseRequest],
    ) -> tuple[dask.dataframe.DataFrame, str]:
        return (
            self.processor.transform(
                raw_path=raw_path_request[0],
                request=raw_path_request[1],
                geo_filter=self.geo_filter,
            ),
            raw_path_request[1].file_name,
        )

    def _store(self, ddf_file_name: tuple[dask.dataframe.DataFrame, str]) -> Path:
        return self.storer.store(
            ddf=ddf_file_name[0],
            destination_path=self.processed_dir / ddf_file_name[1],
        )

    def _build_dask_pipeline(
        self,
        all_new_requests: list[BaseRequest],
        npartitions: int = DEFAULT_NPARTITIONS,
    ) -> dask.bag:
        """Build a dask depenceny graph."""
        return (
            dask.bag.from_sequence(
                seq=all_new_requests,
                npartitions=npartitions,
            )
            .map_partitions(
                self._download_raw_file,
            )  # download files in paralell
            .map(
                self._transform,
            )  # transform each downloaded file
            .map(
                self._store,
            )  # store each transformed dataframe
        )

    def _is_valid_file(self, path: Path, min_size_bytes: float = MIN_VALID_SIZE_BYTES) -> bool:
        """Helper function to check if a file is valid."""
        if self.storer.exists(path=path):
            return path.stat().st_size > min_size_bytes
        return False

    def download_datasets(self, start: dt.date, date_offset: int) -> list[Path]:
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
            if not self._is_valid_file(path=self.processed_dir / t.file_name)
        ]

        logger.debug(
            event="Built requests.",
            count_new_requests=len(all_new_requests),
            count_all_requests=len(all_requests),
        )

        # define the pipeline
        pipeline = (
            self._build_dask_pipeline(all_new_requests) if len(all_new_requests) > 0 else None
        )
        # run the pipeline
        processed_files: list[Path] = (
            pipeline.compute(scheduler="single-threaded") if pipeline is not None else []
        )

        end_time = time.perf_counter()
        logger.info(
            event="Download datasets: END",
            count_processed_files=len(processed_files),
            elapsed_time_secs=end_time - start_time,
        )

        return processed_files
