from pyspark.sql import SparkSession
from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    def __init__(self, spark: SparkSession, source_data: str, file_format: str):
        self.spark = spark
        self.source_data = source_data
        self.file_format = file_format

    @abstractmethod
    def read_file(self, path: str):
        pass

    def execute(self):
        dataframe = {}
        for source in self.source_data:
            df_name = source["df"]
            df = self.read_file(source["path"])
            dataframe[df_name] = df
        return dataframe
