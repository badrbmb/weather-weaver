import datetime as dt
from abc import ABC, abstractmethod, abstractproperty
from typing import Generator

from pydantic import BaseModel

from weather_weaver.models.geo import GeoFilterModel


class BaseRequest(ABC, BaseModel):
    @abstractproperty
    def file_name(self) -> str:
        """File name based on metadata."""
        pass


class BaseRequestBuilder(ABC):
    def __init__(self, *, geo_filter: GeoFilterModel) -> None:
        self.geo_filter = geo_filter

    @abstractmethod
    def build_default_requests(
        self,
        run_date: dt.date,
    ) -> list[BaseRequest]:
        """Build a series of default requests compatible with a fetcher."""
        pass

    @abstractmethod
    def build_closest_requests(
        self,
        target_run_date: dt.datetime,
    ) -> Generator[BaseRequest, None, None]:
        """Builds the request closest to the desired target_run_date."""
        pass
