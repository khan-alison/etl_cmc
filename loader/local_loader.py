from loader.base_loader import BaseLoader
from typing import Dict
from pyspark.sql import DataFrame


class LocalLoader(BaseLoader):
    def __init__(self, output_path: str, output_format: str = "parquet"):
        self.output_path = output_path
        self.output_format = output_format.lower()

    def load(self, dataframes: Dict[str, DataFrame]):
        valid_formats = {"csv", "parquet", "json"}

        if self.output_format not in valid_formats:
            raise ValueError(
                f"Unsupported format: {self.output_format}. Supported formats: {valid_formats}")

        for name, df in dataframes.items():
            output_file = os.path.join(
                self.output_path, f"{name}.{self.output_format}")
            if output_format == "csv":
                df.write.mode("overwrite").option(
                    "header", True).csv(output_file)
