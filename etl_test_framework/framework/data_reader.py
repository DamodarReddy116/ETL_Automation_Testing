from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


class DataReader:
    """Reusable Spark reader for source files and Databricks tables."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_csv(
        self,
        path: str,
        delimiter: str = ",",
        header: bool = True,
        infer_schema: bool = True,
    ) -> DataFrame:
        return (
            self.spark.read.option("header", str(header).lower())
            .option("delimiter", delimiter)
            .option("inferSchema", str(infer_schema).lower())
            .csv(path)
        )

    def read_txt(self, path: str, delimiter: str = "|", header: bool = True) -> DataFrame:
        return (
            self.spark.read.option("header", str(header).lower())
            .option("delimiter", delimiter)
            .option("inferSchema", "true")
            .csv(path)
        )

    def read_table(self, table_name: str) -> DataFrame:
        return self.spark.table(table_name)

    def read_sql(self, query: str) -> DataFrame:
        return self.spark.sql(query)
