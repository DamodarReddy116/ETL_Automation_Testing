from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, functions as F

CATALOG = "etl_demo"
RAW_SCHEMA = "bronze"
OP_SCHEMA = "silver"
CUR_SCHEMA = "gold"

RAW_TABLE = f"{CATALOG}.{RAW_SCHEMA}.raw_customer_data"
OP_TABLE = f"{CATALOG}.{OP_SCHEMA}.operational_customer_data"
CUR_TABLE = f"{CATALOG}.{CUR_SCHEMA}.curated_customer_metrics"

LOCAL_SOURCE = str(Path(__file__).resolve().parent / "data" / "customer_source.csv")
DBFS_SOURCE = "dbfs:/FileStore/etl_demo/customer_source.csv"


def create_catalog_and_schemas(spark) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OP_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CUR_SCHEMA}")


def upload_csv_to_dbfs(dbutils) -> None:
    try:
        dbutils.fs.ls("dbfs:/FileStore/etl_demo")
    except Exception:
        dbutils.fs.mkdirs("dbfs:/FileStore/etl_demo")

    dbutils.fs.cp(f"file:{LOCAL_SOURCE}", DBFS_SOURCE, recurse=False)
    print(f"Uploaded CSV to {DBFS_SOURCE}")


def read_raw_csv(spark) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .csv(DBFS_SOURCE)
    )


def standardize_raw_df(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("signup_date", F.to_date(F.col("signup_date")))
        .withColumn("amount", F.round(F.abs(F.col("amount").cast("double")), 2))
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
        .withColumn("region", F.upper(F.trim(F.col("region"))))
        .withColumn("source_system", F.upper(F.trim(F.col("source_system"))))
    )


def create_operational_df(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn("full_name", F.lower(F.trim(F.concat_ws(" ", F.col("first_name"), F.col("last_name")))))
        .withColumn("email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1))
        .withColumn("has_valid_email", F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("amount", F.round(F.abs(F.col("amount").cast("double")), 2))
        .withColumn("is_active", F.col("status").isin("ACTIVE", "PENDING", "NEW"))
        .withColumn("is_high_value", F.col("amount") >= 500)
        .withColumn("signup_month", F.date_format(F.col("signup_date"), "yyyy-MM"))
        .withColumn("customer_age_days", F.datediff(F.current_date(), F.col("signup_date")))
        .withColumn(
            "customer_tier",
            F.when(F.col("amount") >= 500, F.lit("GOLD"))
            .when(F.col("amount") >= 200, F.lit("SILVER"))
            .otherwise(F.lit("BRONZE")),
        )
        .withColumn(
            "risk_flag",
            F.when(F.col("amount") >= 1000, F.lit("HIGH"))
            .when(F.col("amount") >= 500, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW")),
        )
        .withColumn(
            "customer_segment",
            F.when(F.col("is_high_value") & F.col("is_active"), F.lit("VIP"))
            .when(F.col("is_active"), F.lit("LOYAL"))
            .when(F.col("amount") >= 100, F.lit("REGULAR"))
            .otherwise(F.lit("NEW")),
        )
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
        .select(
            "customer_id",
            "full_name",
            "email",
            "email_domain",
            "has_valid_email",
            "status",
            "signup_date",
            "amount",
            "customer_tier",
            "risk_flag",
            "customer_segment",
            "is_active",
            "is_high_value",
            "signup_month",
            "customer_age_days",
            "updated_at",
        )
    )


def create_curated_df(operational_df: DataFrame) -> DataFrame:
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
            F.max(F.when(F.col("risk_flag") == "HIGH", F.lit(1)).otherwise(F.lit(0))).alias("review_flag"),
        )
        .withColumn(
            "customer_tier",
            F.when(F.col("total_amount") >= 500, F.lit("GOLD"))
            .when(F.col("total_amount") >= 200, F.lit("SILVER"))
            .otherwise(F.lit("BRONZE")),
        )
        .drop("customer_tier_raw")
    )


def write_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    df.write.format("delta").mode(mode).saveAsTable(table_name)


def main() -> None:
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("Spark session is not active.")

        dbutils = globals().get("dbutils")
        if dbutils is None:
            raise RuntimeError("dbutils is not available in this execution context.")
    except Exception as exc:
        raise RuntimeError(
            "This script is intended for a Databricks notebook environment with access to spark and dbutils. "
            "Run it in a Databricks notebook or cluster attached to this workspace."
        ) from exc

    create_catalog_and_schemas(spark)
    upload_csv_to_dbfs(dbutils)

    raw_df = read_raw_csv(spark)
    raw_df = standardize_raw_df(raw_df)
    write_delta_table(raw_df, RAW_TABLE)
    print(f"Raw data loaded to {RAW_TABLE}")

    operational_df = create_operational_df(raw_df)
    write_delta_table(operational_df, OP_TABLE)
    print(f"Operational data loaded to {OP_TABLE}")

    curated_df = create_curated_df(operational_df)
    write_delta_table(curated_df, CUR_TABLE)
    print(f"Curated metrics loaded to {CUR_TABLE}")


if __name__ == "__main__":
    main()
