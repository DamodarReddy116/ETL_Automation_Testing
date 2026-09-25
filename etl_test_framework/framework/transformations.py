from __future__ import annotations

from typing import Dict, Iterable, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_customer_transformations(raw_df: DataFrame) -> DataFrame:
    """Apply the core production ETL cleaning and enrichment rules for customer data."""
    return (
        raw_df
        .withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
        .withColumn("first_name", F.trim(F.regexp_replace(F.col("first_name"), r"\s+", " ")))
        .withColumn("last_name", F.trim(F.regexp_replace(F.col("last_name"), r"\s+", " ")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("full_name", F.trim(F.concat_ws(" ", F.col("first_name"), F.col("last_name"))))
        .withColumn("signup_date", F.to_date(F.col("signup_date")))
        .withColumn("amount", F.when(F.col("amount").isNull(), F.lit(0.0)).otherwise(F.round(F.abs(F.col("amount").cast("double")), 2)))
        .withColumn("is_active", F.when(F.col("status").isin("ACTIVE", "PENDING", "NEW"), F.lit(True)).otherwise(F.lit(False)))
        .withColumn("email_domain", F.when(F.col("email").contains("@"), F.split(F.col("email"), "@")[1]).otherwise(F.lit(None)))
        .withColumn("has_valid_email", F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"))
        .withColumn("signup_month", F.date_format(F.col("signup_date"), "yyyy-MM"))
        .withColumn("customer_age_days", F.when(F.col("signup_date").isNotNull(), F.datediff(F.current_date(), F.col("signup_date"))).otherwise(F.lit(None)))
        .withColumn("customer_tier", F.when(F.col("amount") >= 500, F.lit("GOLD")).when(F.col("amount") >= 200, F.lit("SILVER")).otherwise(F.lit("BRONZE")))
        .withColumn("risk_flag", F.when((F.col("amount") >= 1000) | (F.col("status") == "INACTIVE"), F.lit("HIGH")).when((F.col("amount") >= 500) | (F.col("status") == "PENDING"), F.lit("MEDIUM")).otherwise(F.lit("LOW")))
        .withColumn("customer_segment", F.when(F.col("amount") >= 1000, F.lit("VIP")).when(F.col("amount") >= 500, F.lit("LOYAL")).when(F.col("amount") >= 100, F.lit("REGULAR")).otherwise(F.lit("NEW")))
        .withColumn("is_high_value", F.col("amount") >= 500)
    )


def apply_advanced_customer_transformations(raw_df: DataFrame) -> DataFrame:
    """Apply the full production pipeline set: cleaning, null remediation, dates, flags, enrichments, audits."""
    df = standardize_customer_columns(raw_df)
    df = apply_null_remediation(df, {"first_name": "UNKNOWN", "last_name": "UNKNOWN", "email": "unknown@missing.com", "status": "NEW"})
    df = add_audit_columns(df, batch_id="BATCH_001", source_system="CRM")
    df = add_date_dimensions(df, date_col="signup_date")
    df = apply_customer_transformations(df)
    df = add_business_flags(df)
    return df


def standardize_customer_columns(df: DataFrame) -> DataFrame:
    """Normalize common text fields used across source data and downstream analytics."""
    return (
        df.withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
        .withColumn("first_name", F.trim(F.regexp_replace(F.col("first_name"), r"\s+", " ")))
        .withColumn("last_name", F.trim(F.regexp_replace(F.col("last_name"), r"\s+", " ")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
    )


def apply_null_remediation(df: DataFrame, fill_map: Optional[Dict[str, str]] = None) -> DataFrame:
    """Fill null values for known operational columns to avoid downstream issues."""
    fill_map = fill_map or {"first_name": "UNKNOWN", "last_name": "UNKNOWN", "email": "unknown@missing.com", "status": "NEW"}
    for column_name, default_value in fill_map.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, F.when(F.col(column_name).isNull(), F.lit(default_value)).otherwise(F.col(column_name)))
    return df


def deduplicate_customer_records(df: DataFrame, key_columns: Iterable[str]) -> DataFrame:
    """Remove duplicates using business keys; common in real-time ETL to prevent repeated rows."""
    key_columns = list(key_columns)
    return df.dropDuplicates(subset=key_columns)


def add_audit_columns(df: DataFrame, batch_id: str = "batch_001", source_system: str = "unknown") -> DataFrame:
    """Add metadata used to trace ingestion and debugging across ETL jobs."""
    return (
        df.withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_system", F.lit(source_system))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
    )


def add_date_dimensions(df: DataFrame, date_col: str = "signup_date") -> DataFrame:
    """Derive calendar fields for effective date-based reporting and segmentation."""
    if date_col not in df.columns:
        return df
    return (
        df.withColumn(date_col, F.to_date(F.col(date_col)))
        .withColumn("signup_year", F.year(F.col(date_col)))
        .withColumn("signup_month", F.date_format(F.col(date_col), "yyyy-MM"))
        .withColumn("signup_quarter", F.concat(F.lit("Q"), F.quarter(F.col(date_col))))
        .withColumn("signup_day_of_week", F.date_format(F.col(date_col), "EEEE"))
        .withColumn("signup_week_of_year", F.weekofyear(F.col(date_col)))
        .withColumn("is_weekend", F.when(F.dayofweek(F.col(date_col)).isin(1, 7), F.lit(True)).otherwise(F.lit(False)))
    )


def enrich_with_reference_data(df: DataFrame, lookup_df: DataFrame, join_key: str = "customer_id", lookup_key: str = "customer_id", value_col: str = "customer_segment") -> DataFrame:
    """Join a master/reference dataset for enrichment and classification lookups."""
    return (
        df.join(
            lookup_df.select(lookup_key, value_col),
            df[join_key] == lookup_df[lookup_key],
            "left",
        )
        .withColumn(value_col, F.coalesce(F.col(value_col), F.lit("UNKNOWN")))
        .drop(lookup_key)
    )


def create_incremental_load_markers(df: DataFrame, load_date: Optional[str] = None) -> DataFrame:
    """Add markers that are useful for incremental loads, backfills, and snapshot testing."""
    effective_load_date = load_date or F.current_date().cast("string")
    return df.withColumn("load_date", F.lit(effective_load_date)).withColumn("is_latest_load", F.lit(True))


def add_business_flags(df: DataFrame) -> DataFrame:
    """Create common operational flags used during validation and reporting."""
    return (
        df.withColumn("has_valid_email", F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"))
        .withColumn("is_recent_signup", F.when(F.col("signup_date") >= F.date_sub(F.current_date(), 30), F.lit(True)).otherwise(F.lit(False)))
        .withColumn("is_high_value", F.col("amount") >= 500)
        .withColumn("needs_review", F.when((F.col("has_valid_email") == False) | (F.col("status") == "PENDING"), F.lit(True)).otherwise(F.lit(False)))
    )


def build_curated_customer_metrics(operational_df: DataFrame) -> DataFrame:
    """Aggregate operational data into curated KPI-level output, including real production metrics."""
    return (
        operational_df.groupBy("customer_id")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.max("signup_date").alias("latest_signup_date"),
            F.min("signup_date").alias("first_signup_date"),
            F.count("customer_id").alias("total_records"),
            F.sum(F.when(F.col("is_active"), F.lit(1)).otherwise(F.lit(0))).alias("active_record_count"),
            F.sum(F.when(F.col("is_high_value"), F.lit(1)).otherwise(F.lit(0))).alias("high_value_record_count"),
            F.first("customer_tier").alias("customer_tier_raw"),
            F.max(F.when(F.col("needs_review"), F.lit(1)).otherwise(F.lit(0))).alias("review_flag"),
        )
        .withColumn("customer_tier", F.when(F.col("total_amount") >= 500, F.lit("GOLD")).when(F.col("total_amount") >= 200, F.lit("SILVER")).otherwise(F.lit("BRONZE")))
        .drop("customer_tier_raw")
    )


def create_cdc_incremental_load(current_df: DataFrame, incoming_df: DataFrame, key_col: str = "customer_id", timestamp_col: str = "updated_at") -> DataFrame:
    """Return the upsert delta rows from incoming data that are newer than the current version."""
    current_key_df = current_df.select(key_col, timestamp_col).withColumnRenamed(timestamp_col, f"{timestamp_col}_current")
    delta = incoming_df.join(current_key_df, key_col, "left")
    result = (
        delta.filter(
            F.col(f"{timestamp_col}_current").isNull() |
            (F.col(timestamp_col) > F.col(f"{timestamp_col}_current"))
        )
        .withColumn("row_action", F.lit("UPSERT"))
        .drop(f"{timestamp_col}_current")
    )
    return result


def apply_scd_type_2(
    dim_df: DataFrame,
    incoming_df: DataFrame,
    business_key: str = "customer_id",
    change_col: str = "status",
    effective_start_col: str = "valid_from",
    effective_end_col: str = "valid_to",
    is_current_col: str = "is_current",
    version_col: str = "version_no",
) -> DataFrame:
    """Create a simple SCD Type 2 history record set based on changed business attributes."""
    spark = dim_df.sparkSession
    current_rows = dim_df.filter(F.col(is_current_col) == True).collect()
    incoming_rows = incoming_df.collect()

    result_rows = []
    current_by_key = {row[business_key]: row for row in current_rows}
    incoming_keys = set()

    for incoming_row in incoming_rows:
        incoming_data = incoming_row.asDict()
        key_value = incoming_data[business_key]
        incoming_keys.add(key_value)
        current_row = current_by_key.get(key_value)
        effective_from = incoming_data.get("updated_at")

        if current_row is None:
            new_row = {business_key: key_value, change_col: incoming_data[change_col], effective_start_col: effective_from, effective_end_col: None, is_current_col: True, version_col: 1}
            result_rows.append(new_row)
            continue

        current_data = current_row.asDict()
        if current_data[change_col] != incoming_data[change_col]:
            expired_row = dict(current_data)
            expired_row[effective_end_col] = effective_from
            expired_row[is_current_col] = False
            result_rows.append(expired_row)

            new_row = dict(current_data)
            new_row[change_col] = incoming_data[change_col]
            new_row[effective_start_col] = effective_from
            new_row[effective_end_col] = None
            new_row[is_current_col] = True
            new_row[version_col] = (current_data[version_col] if current_data[version_col] is not None else 1) + 1
            result_rows.append(new_row)
        else:
            result_rows.append(dict(current_data))

    for current_row in current_rows:
        key_value = current_row[business_key]
        if key_value not in incoming_keys:
            result_rows.append(dict(current_row.asDict()))

    if not result_rows:
        return dim_df

    output_df = spark.createDataFrame(result_rows)
    return output_df


def reconcile_fact_dimension(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_key: str = "customer_id",
    dim_key: str = "customer_id",
    metric_col: str = "amount",
) -> DataFrame:
    """Return fact rows that do not have a matching dimension key; useful for referential integrity checks."""
    valid_dim_keys = dim_df.select(dim_key).distinct()
    invalid_fact_rows = fact_df.join(valid_dim_keys, fact_df[fact_key] == valid_dim_keys[dim_key], "left_anti")
    return invalid_fact_rows.orderBy(fact_key)


def build_sales_order_transformations(order_df: DataFrame) -> DataFrame:
    """Apply common sales-order transformations used in operational finance and reporting jobs."""
    return (
        order_df
        .withColumn("order_id", F.upper(F.trim(F.col("order_id"))))
        .withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
        .withColumn("order_date", F.to_date(F.col("order_date")))
        .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
        .withColumn("gross_amount", F.when(F.col("gross_amount").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("gross_amount").cast("double"))))
        .withColumn("discount_amount", F.when(F.col("discount_amount").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("discount_amount").cast("double"))))
        .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
        .withColumn("net_amount", F.when(F.col("net_amount") < 0, F.lit(0.0)).otherwise(F.col("net_amount")))
        .withColumn("order_month", F.date_format(F.col("order_date"), "yyyy-MM"))
        .withColumn("status_flag", F.when(F.col("order_status").isin("DELIVERED", "SHIPPED"), F.lit("COMPLETED")).when(F.col("order_status").isin("PENDING", "PROCESSING"), F.lit("PENDING")).otherwise(F.lit("REVIEW")))
        .withColumn("order_value_bucket", F.when(F.col("net_amount") >= 500, F.lit("HIGH")).when(F.col("net_amount") >= 100, F.lit("MEDIUM")).otherwise(F.lit("LOW")))
    )
