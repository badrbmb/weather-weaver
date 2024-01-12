import datetime as dt
import time
from pathlib import Path

import dask
import dask.dataframe
import dask_geopandas as dask_gpd
import structlog
from pandas.tseries.offsets import DateOffset

from weather_weaver.models.fetcher import FetcherInterface
from weather_weaver.models.processor import BaseProcessor
from weather_weaver.models.request import BaseRequest, BaseRequestBuilder
from weather_weaver.models.storage import StorageInterface
from weather_weaver.utils import OffsetFrequency

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

    @staticmethod
    def _date_offset(offset_frequency: OffsetFrequency, offset: int) -> DateOffset:
        match offset_frequency:
            case OffsetFrequency.DAILY:
                return DateOffset(days=offset)
            case OffsetFrequency.YEARLY:
                return DateOffset(years=offset)
            case _:
                raise NotImplementedError(f"{offset_frequency=} not implemented yet!")

    def _build_default_requests(
        self,
        start: dt.date,
        date_offset: int,
        offset_frequency: OffsetFrequency,
    ) -> list[BaseRequest]:
        all_run_dates: list[dt.datetime] = [
            (
                start
                + self._date_offset(
                    offset=i,
                    offset_frequency=offset_frequency,
                )
            )
            .to_pydatetime()
            .date()
            for i in range(date_offset)
        ]
        all_requests: list[list[BaseRequest]] = [
            self.request_builder.build_default_requests(run_date=run_date)
            for run_date in all_run_dates
        ]
        # flatten and return list
        return [u for v in all_requests for u in v]

    def _filter_new_requests(self, all_requests: list[BaseRequest]) -> list[BaseRequest]:
        return [
            t
            for t in all_requests
            if not self.storer.is_valid(
                path=self.processed_dir / f"{t.file_name}.parquet",
            )
        ]

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
            geo_filter=self.request_builder.geo_filter,
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

    def _process_requests(
        self,
        all_new_requests: list[BaseRequest],
        dask_scheduler: str | None = None,
    ) -> list[Path]:
        # TODO: do we need an orchestrator here instead?
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
        processed_files: list[Path] = (
            pipeline.compute(scheduler=dask_scheduler) if pipeline is not None else []
        )
        return processed_files

    def download_datasets(
        self,
        start: dt.date,
        date_offset: int,
        offset_frequency: OffsetFrequency,
        scheduler: str | None = None,
    ) -> list[Path]:
        """Download datasets for all dates between start and end."""
        start_time = time.perf_counter()
        logger.info(
            event="Download datasets: START",
            start=start,
            date_offset=date_offset,
            offset_frequency=offset_frequency.value,
        )
        all_requests = self._build_default_requests(
            start=start,
            date_offset=date_offset,
            offset_frequency=offset_frequency,
        )

        # check the ones already processed
        all_new_requests = self._filter_new_requests(all_requests)

        logger.debug(
            event="Built requests.",
            count_new_requests=len(all_new_requests),
            count_all_requests=len(all_requests),
        )

        processed_files = self._process_requests(all_new_requests, dask_scheduler=scheduler)

        end_time = time.perf_counter()
        logger.info(
            event="Download datasets: END",
            count_processed_files=len(processed_files),
            count_failed_files=len(all_new_requests) - len(processed_files),
            elapsed_time_secs=end_time - start_time,
        )

        return processed_files
