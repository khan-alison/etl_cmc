from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)

class BaseLoader(ABC):
    """Abstract base class for loaders"""

    @abstractmethod
    def load(self, dataframes: Dict[str, DataFrame]):
        """Load data into a specific storage"""
        pass