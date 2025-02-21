from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Dict
from helper.logger import LoggerSimple
from pyspark.sql.window import Window


logger = LoggerSimple.get_logger(__name__)


class Transformer:
    def __init__(self):
        self.transformations = {
            "total_employees_per_dept": self.calculate_total_employees_per_dept,
            "salary_category": self.categorize_salary,
            "total_salary_per_dept": self.total_salary_per_dept,
            "highest_salary_per_dept": self.highest_salary_per_dept
        }
        self.dataframes = {}

    def transform(self, dataframes: Dict[str, DataFrame]):
        self.dataframes = dataframes

        transformed_data = {}

        for name, func in self.transformations.items():
            try:
                transformed_data[name] = func()
                logger.info(f"Successfully transformed: {name}")
            except KeyError as e:
                logger.warning(f"Skipping {name}: Missing required data - {e}")

        return transformed_data

    def calculate_total_employees_per_dept(self):
        if self.dataframes["employee_df"] is None:
            raise KeyError("employee_df")
        if self.dataframes["department_df"] is None:
            raise KeyError("department_df")

        return self.dataframes["department_df"].alias("d")\
            .join(self.dataframes["employee_df"].alias("e"),
                  on=(F.col("d.department_id") == F.col("e.department_id")),
                  how="left").groupBy([F.col("d.department_id"), F.col("d.name")])\
            .agg(F.count("*").alias("total_employees"))\
            .select(
            F.col("d.department_id"),
            F.col("d.name").alias("department_name"),
            F.col("total_employees"),
        ).limit(3)

    def categorize_salary(self):
        if self.dataframes["employee_df"] is None:
            raise KeyError("employee_df")

        return self.dataframes["employee_df"].alias("e").withColumn(
            "salary_categories",
            F.when((F.col("e.salary") > 0) & (
                F.col("e.salary") <= 1000), "junior")
            .when((F.col("e.salary") > 1000) & (
                F.col("e.salary") <= 1500), "middle")
            .when((F.col("e.salary") >= 1500), "senior")
            .otherwise("No data")
        ).limit(3)

    def total_salary_per_dept(self):
        if self.dataframes["employee_df"] is None:
            raise KeyError("employee_df")
        if self.dataframes["department_df"] is None:
            raise KeyError("department_df")

        return self.dataframes["department_df"].alias("d")\
            .join(self.dataframes["employee_df"].alias("e"),
                  on=(F.col("d.department_id") == F.col("e.department_id")),
                  how="left")\
            .groupBy([F.col("d.department_id"), F.col("d.name")])\
            .agg(F.sum(F.col("e.salary"))).alias("total_salary").limit(3)

    def highest_salary_per_dept(self):
        if self.dataframes["employee_df"] is None:
            raise KeyError("employee_df")
        if self.dataframes["department_df"] is None:
            raise KeyError("department_df")

        return self.dataframes["department_df"].alias("d")\
            .join(self.dataframes["employee_df"].alias("e"),
                  on=(F.col("d.department_id") == F.col("e.department_id")),
                  how="left")\
           .withColumn(
            "rank",
            F.rank().over(
                Window.partitionBy(F.col("d.department_id")).orderBy(
                    F.desc(F.col("e.salary"))
                )
            )
        ).where(F.col("rank") == 1)\
            .select(
                F.col("d.department_id"),
            F.col("d.name").alias("department_name"),
            F.col("e.name").alias("employee_name"),
            F.col("e.salary"),
            F.col("rank"),
        ).limit(3)
