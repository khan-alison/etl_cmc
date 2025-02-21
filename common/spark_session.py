import os
from pyspark.sql import SparkSession
from helper.logger import LoggerSimple
from dotenv import load_dotenv
import boto3

logger = LoggerSimple.get_logger(__name__)
load_dotenv()


class SparkSessionManager:
    _instance = None

    def __init__(self, appName: str, master: str):
        if SparkSessionManager._instance is not None:
            raise Exception(
                "This is a singleton! Use `get_session()` method instead.")
        self.appName = appName
        self.master = master
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY")
        self.aws_secret_key = os.getenv("AWS_SECRET_KEY")
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.s3_region = self.get_s3_region()
        self.spark = self._create_spark_session()
        SparkSessionManager._instance = self
        logger.info(
            f"✅ Connected to S3 Bucket: {self.bucket_name} in region: {self.s3_region}")

    def _create_spark_session(self):
        project_root = os.path.dirname(os.path.abspath(__file__))
        jars_dir = os.path.join(project_root, "../jars")
        jar_files = [os.path.join(jars_dir, jar) for jar in os.listdir(
            jars_dir) if jar.endswith(".jar")]
        jars_str = ",".join(jar_files)

        logger.info(f"Loading JARs from: {jars_str}")
        builder = (
            SparkSession.builder
            .appName(self.appName)
            .master(self.master)
            .config("spark.jars", jars_str)
            .config("spark.hadoop.fs.s3a.access.key", self.aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.aws_secret_key)
            .config("spark.sql.shuffle.partitions", 5)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        )
        spark = builder.getOrCreate()
        return spark

    @staticmethod
    def get_session(appName: str, master: str):
        if SparkSessionManager._instance is None:
            SparkSessionManager(appName, master)
        return SparkSessionManager._instance.spark

    @staticmethod
    def close_session():
        if SparkSessionManager._instance is not None:
            SparkSessionManager._instance.spark.stop()
            SparkSessionManager._instance = None
            logger.info("Spark session close")

    
    def get_s3_region(self):
        """Fetches the region of an S3 bucket"""
        s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
        bucket_location = s3_client.get_bucket_location(
            Bucket=self.bucket_name)
        return bucket_location['LocationConstraint'] if bucket_location['LocationConstraint'] else 'us-east-1'
