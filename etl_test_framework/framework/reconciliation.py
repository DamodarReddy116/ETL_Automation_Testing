from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame


class Reconciliation:
    """Key-based recovery and reconciliation checks for source and target datasets."""

    @staticmethod
    def validate_key_reconciliation(
        source_df: DataFrame,
        target_df: DataFrame,
        key_columns: Iterable[str],
    ) -> None:
        key_columns = list(key_columns)

        source_keys = source_df.select(*key_columns).distinct()
        target_keys = target_df.select(*key_columns).distinct()

        missing = source_keys.join(target_keys, key_columns, "left_anti")
        unexpected = target_keys.join(source_keys, key_columns, "left_anti")

        missing_count = missing.count()
        unexpected_count = unexpected.count()

        assert missing_count == 0, (
            f"{missing_count} source keys are missing from target for {key_columns}."
        )
        assert unexpected_count == 0, (
            f"{unexpected_count} unexpected keys found in target for {key_columns}."
        )
