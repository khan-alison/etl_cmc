from helper.logger import LoggerSimple
from common.spark_session import SparkSessionManager
from extract.local_extract import LocalExtractor
from extract.base_extract import BaseExtractor
from extract.s3_extract import S3Extractor
from loader.base_loader import BaseLoader
from loader.local_loader import LocalLoader
from loader.dynamo_loader import DynamoDBLoader
from transformer.transformer import Transformer
from pyspark.sql.types import *


logger = LoggerSimple.get_logger(__name__)


class Executor:
    def __init__(self, spark: SparkSessionManager, extractor: BaseExtractor, transformer: Transformer, loader: BaseLoader):
        self.spark = spark
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def extract_data(self):
        return self.extractor.execute()

    def transform_data(self, df):
        return self.transformer.transform(df)

    def load_data(self, df):
        return self.loader.load(df)

    def execute(self):
        raw_data = self.extract_data()
        transformed_data = self.transform_data(raw_data)
        self.load_data(transformed_data)


if __name__ == "__main__":
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("position", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("department_id", IntegerType(), True)
    ])
    department_schema = StructType([
        StructField("department_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True)
    ])
    configuration = {
        "appName": "test",
        "master": "local",
        "input_format": "csv",
        "source_type": "s3",
        "source_data": [
            {"df": "employee_df", "path": "raw/employee.csv",
                "schema": employee_schema},
            {"df": "department_df", "path": "raw/department.csv",
                "schema": department_schema}
        ],
        "storage_type": "dynamodb",
        "dynamodb_table": "MyDynamoDBTableName",
        "output_format": "parquet",
        "output_path": "data/transformed"
    }

    spark = SparkSessionManager.get_session(
        configuration["appName"],
        configuration["master"]
    )

    if configuration["source_type"] == "s3":
        extractor = S3Extractor(
            spark=spark,
            source_data=configuration["source_data"],
            file_format=configuration["input_format"]
        )
    else:
        extractor = LocalExtractor(
            spark=spark,
            source_data=configuration["source_data"],
            file_format=configuration["input_format"]
        )

    transformer = Transformer()
    if configuration["storage_type"] == "dynamodb":
        loader = DynamoDBLoader(configuration["dynamodb_table"])
    else:
        loader = LocalLoader(
            configuration["output_path"], configuration["output_format"])
    executor = Executor(spark=spark, extractor=extractor,
                        transformer=transformer, loader=loader)
    dataframes = executor.execute()
