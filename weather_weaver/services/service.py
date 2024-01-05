import datetime as dt
from pathlib import Path

import dask
import structlog

from weather_weaver.models.fetcher import FetcherInterface
from weather_weaver.models.processor import BaseProcessor
from weather_weaver.models.request import BaseRequest, BaseRequestBuilder
from weather_weaver.models.storage import StorageInterface

dask.config.set({"array.slicing.split_large_chunks": True})
logger = structlog.getLogger()


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

    def download_datasets(self, start: dt.date, date_offset: int) -> list[Path]:
        """Download datasets for all dates between start and end."""
        logger.info(
            event="Download datasets: START",
            start=start,
            date_offset=date_offset,
        )
        all_requests = self._build_default_requests(start=start, date_offset=date_offset)

        # check the ones already processed
        all_new_requests = [
            t for t in all_requests if not self.storer.exists(path=self.processed_dir / t.file_name)
        ]

        logger.debug(
            event="Built requests",
            request_count=len(all_requests),
            new_requests=len(all_new_requests),
        )

        for request in all_new_requests:
            raw_file = self.fetcher.download_raw_file(
                request=request,
                raw_dir=self.raw_dir,
            )
            ddf = self.processor.transform(
                raw_path=raw_file,
                request=request,
            )
            self.storer.store(
                ddf=ddf,
                destination_path=self.processed_dir / request.file_name,
            )
