import datetime as dt
from pathlib import Path

import dask.dataframe as da
import geopandas as gpd
import pytest

from weather_weaver.services.service import WeatherConsumerService
from weather_weaver.utils import OffsetFrequency


class MockRequest:
    def __init__(self, run_date: dt.date):
        self.file_name = run_date.strftime("%Y%m%d")

    def __eq__(self, other):
        if isinstance(other, MockRequest):
            return self.file_name == other.file_name
        return False


@pytest.fixture
def request_builder_mock(mocker):
    def mock_request_builder(run_date: dt.date):
        return [MockRequest(run_date)]

    request_builder_mock = mocker.Mock()
    request_builder_mock.build_default_requests.side_effect = mock_request_builder
    return request_builder_mock


@pytest.fixture
def fetcher_mock(mocker):
    def mock_download_raw_file(raw_dir: Path, request: MockRequest) -> Path:
        return raw_dir / f"{request.file_name}.grib2"

    fetcher_mock = mocker.Mock()
    fetcher_mock.download_raw_file.side_effect = mock_download_raw_file
    return fetcher_mock


@pytest.fixture
def processor_mock(mocker):
    processor_mock = mocker.Mock()
    processor_mock.transform.return_value = gpd.GeoDataFrame()
    return processor_mock


@pytest.fixture
def storer_mock(mocker):
    def mock_store(ddf: da.DataFrame, destination_path: Path):  # noqa: ARG001
        return destination_path

    storer_mock = mocker.Mock()
    storer_mock.store.side_effect = mock_store
    storer_mock.is_valid.return_value = False
    return storer_mock


class TestWeatherConsumerService:
    # Build default requests
    def test_build_default_requests(
        self,
        mocker,
        request_builder_mock,
        fetcher_mock,
        processor_mock,
        storer_mock,
    ):
        # Arrange - Set up inputs and expected outputs
        service = WeatherConsumerService(
            request_builder=request_builder_mock,
            fetcher=fetcher_mock,
            raw_dir=Path("raw_dir"),
            processor=processor_mock,
            storer=storer_mock,
            processed_dir=Path("processed_dir"),
        )
        start = dt.date(2022, 1, 1)
        date_offset = 2
        offset_frequency = OffsetFrequency.DAILY

        # Define the expected output
        expected_requests = [MockRequest(dt.date(2022, 1, 1)), MockRequest(dt.date(2022, 1, 2))]

        # Mock the necessary methods
        request_builder_mock.build_default_requests.return_value = expected_requests

        # Invoke the method under test
        requests = service._build_default_requests(start, date_offset, offset_frequency)

        # Assert the output
        assert requests == expected_requests
        request_builder_mock.build_default_requests.assert_has_calls(
            [mocker.call(run_date=dt.date(2022, 1, 1)), mocker.call(run_date=dt.date(2022, 1, 2))],
        )

    # Download datasets for all dates between start and end
    def test_download_datasets(
        self,
        mocker,
        request_builder_mock,
        fetcher_mock,
        processor_mock,
        storer_mock,
    ):
        # Arrange - Set up inputs and expected outputs
        service = WeatherConsumerService(
            request_builder=request_builder_mock,
            fetcher=fetcher_mock,
            raw_dir=Path("raw_dir"),
            processor=processor_mock,
            storer=storer_mock,
            processed_dir=Path("processed_dir"),
        )
        start = dt.date(2022, 1, 1)
        date_offset = 1
        offset_frequency = OffsetFrequency.DAILY
        mock_request_1 = MockRequest(start)
        expected_processed_files = [
            Path(f"processed_dir/{mock_request_1.file_name}.parquet"),
        ]

        # Act - Call the method under test
        processed_files = service.download_datasets(
            start,
            date_offset,
            offset_frequency,
            scheduler="synchronous",
        )

        # Assert - Check if the expected results are obtained
        assert processed_files == expected_processed_files
        service.request_builder.build_default_requests.assert_called_once_with(
            run_date=start,
        )
        service.storer.is_valid.assert_called_once_with(
            path=Path(f"processed_dir/{mock_request_1.file_name}.parquet"),
        )
        service.fetcher.download_raw_file.assert_called_once_with(
            raw_dir=Path("raw_dir"),
            request=mock_request_1,
        )
        service.processor.transform.assert_called_once_with(
            raw_path=Path("raw_dir") / f"{mock_request_1.file_name}.grib2",
            geo_filter=mocker.ANY,
        )
        service.storer.store.assert_called_once_with(
            ddf=mocker.ANY,
            destination_path=Path(f"processed_dir/{mock_request_1.file_name}.parquet"),
        )
