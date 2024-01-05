import datetime as dt
from pathlib import Path
from typing import Generator

import dask

from weather_weaver.models.fetcher import FetcherInterface
from weather_weaver.models.processor import BaseProcessor
from weather_weaver.models.request import BaseRequest, BaseRequestBuilder
from weather_weaver.models.storage import StorageInterface

dask.config.set({"array.slicing.split_large_chunks": True})


class WeatherConsumerService:
    """Service class for the weather data consumer.

    Each method implements a use case.
    """

    def __init__(
        self,
        *,
        builder: BaseRequestBuilder,
        fetcher: FetcherInterface,
        processor: BaseProcessor,
        storer: StorageInterface,
    ) -> None:
        self.builder = builder
        self.fetcher = fetcher
        self.processor = processor
        self.storer = storer

    def download_datasets(self, start: dt.date, date_offset: int) -> list[Path]:
        """Download datasets for all dates between start and end."""
        all_run_dates: list[dt.datetime] = [
            start + dt.timedelta(days=i) for i in range * date_offset
        ]
        all_request: list[Generator[BaseRequest, None, None]] = [
            self.builder.build_default_requests(run_date=run_date) for run_date in all_run_dates
        ]

        for request_generator in all_request:
            for request in request_generator:
                _ = self.fetcher.download_raw_files(request=request)
