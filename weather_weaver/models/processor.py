from abc import ABC, abstractmethod
from pathlib import Path


class BaseProcessor(ABC):
    @abstractmethod
    def process(self, *, raw_path: Path) -> None:
        """Process a raw file."""
        pass
