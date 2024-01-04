from abc import ABC, abstractproperty

from pydantic import BaseModel


class BaseRequest(ABC, BaseModel):
    @abstractproperty
    def file_name(self) -> str:
        """File name based on metadata."""
        pass
