import boto3
import os
from dotenv import load_dotenv
from typing import Dict
from pyspark.sql import DataFrame
from helper.logger import LoggerSimple

load_dotenv()
logger = LoggerSimple.get_logger(__name__)


class DynamoDBLoader:
    def __init__(self, table_name: str, region_name: str = "us-east-1"):
        self.table_name = table_name
        self.region_name = region_name

        session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )

        self.dynamodb = session.resource("dynamodb")

    def check_or_create_table(self, table_name: str, primary_key: str):
        """Ensure the table exists, otherwise create it."""
        try:
            table = self.dynamodb.Table(table_name)
            table.load()
            logger.info(f"✅ Table '{table_name}' exists.")
            return table
        except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
            logger.info(f"⚠ Table '{table_name}' not found. Creating...")
            return self.create_table(table_name, primary_key)

    def create_table(self, table_name: str, primary_key: str):
        """Create a new DynamoDB table with the given primary key."""
        table = self.dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": primary_key,
                        "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": primary_key, "AttributeType": "S"}],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        table.wait_until_exists()
        logger.info(f"✅ Table '{table_name}' created successfully.")
        return table

    def load(self, dataframes: Dict[str, DataFrame]):
        """Writes each transformed DataFrame to a separate DynamoDB table."""
        table_mappings = {
            "total_employees_per_dept": "department_employees",
            "salary_category": "employee_salary_category",
            "total_salary_per_dept": "department_salary",
            "highest_salary_per_dept": "department_highest_salary"
        }

        for df_name, df in dataframes.items():
            table_name = table_mappings.get(df_name)

            if not table_name:
                logger.warning(
                    f"⚠ No table mapping found for '{df_name}'. Skipping.")
                continue

            primary_key = "department_id" if "department_id" in df.columns else "employee_id"
            table = self.check_or_create_table(table_name, primary_key)

            records = df.collect()
            logger.info(
                f"📤 Writing {len(records)} records to DynamoDB table '{table_name}'...")

            for row in records:
                item = {key: str(value) for key, value in row.asDict(
                ).items() if value is not None}  # Convert to string
                try:
                    table.put_item(Item=item)
                    logger.info(f"✅ Inserted into {table_name}: {item}")
                except Exception as e:
                    logger.error(
                        f"❌ Failed to insert {item} into {table_name}: {str(e)}")

        logger.info(
            f"✅ Successfully wrote transformed data to DynamoDB tables.")
