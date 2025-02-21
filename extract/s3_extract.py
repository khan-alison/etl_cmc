import os

from extract.base_extract import BaseExtractor
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)
load_dotenv()


class S3Extractor(BaseExtractor):
    def __init__(self, spark: SparkSession, source_data: list, file_format: str):
        super().__init__(spark, source_data, file_format)
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")

    def read_file(self, path: str):
        """Reads a file from AWS S3"""
        s3_path = f"s3a://{self.bucket_name}/{path}"
        logger.info((f"📂 Reading file from S3: {s3_path}"))

        return self.spark.read.format(self.file_format)\
            .option("header", True)\
            .option("inferSchema", True)\
            .load(s3_path)
