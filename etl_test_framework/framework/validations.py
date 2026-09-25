from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataValidator:
    """Reusable business and data-quality validation checks."""

    @staticmethod
    def validate_not_empty(df: DataFrame) -> None:
        assert df.limit(1).count() > 0, "Dataset is empty."

    @staticmethod
    def validate_record_count(source_df: DataFrame, target_df: DataFrame) -> None:
        source_count = source_df.count()
        target_count = target_df.count()
        assert source_count == target_count, (
            f"Record count mismatch: source={source_count}, target={target_count}"
        )

    @staticmethod
    def validate_nulls(df: DataFrame, columns: Iterable[str]) -> None:
        for column in columns:
            null_count = df.filter(F.col(column).isNull()).count()
            assert null_count == 0, f"Column '{column}' has {null_count} NULL values."

    @staticmethod
    def validate_duplicates(df: DataFrame, key_columns: Iterable[str]) -> None:
        key_columns = list(key_columns)
        duplicates = (
            df.groupBy(*key_columns).count().filter(F.col("count") > 1)
        )
        duplicate_count = duplicates.count()
        assert duplicate_count == 0, (
            f"Found {duplicate_count} duplicate keys for {key_columns}."
        )

    @staticmethod
    def validate_columns(df: DataFrame, expected_columns: Iterable[str]) -> None:
        missing = set(expected_columns) - set(df.columns)
        assert not missing, f"Missing expected columns: {sorted(missing)}"

    @staticmethod
    def validate_allowed_values(df: DataFrame, column: str, allowed_values: Iterable[str]) -> None:
        invalid_count = df.filter(~F.col(column).isin(list(allowed_values))).count()
        assert invalid_count == 0, (
            f"Column '{column}' contains {invalid_count} rows outside allowed values: {list(allowed_values)}"
        )

    @staticmethod
    def validate_expected_schema(df: DataFrame, expected_schema: dict) -> None:
        for column_name, expected_type in expected_schema.items():
            actual_type = str(df.schema[column_name].dataType)
            assert actual_type.lower() == expected_type.lower(), (
                f"Column '{column_name}' expected type {expected_type} but found {actual_type}"
            )
