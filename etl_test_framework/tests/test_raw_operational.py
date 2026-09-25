from framework.transformations import (
    add_audit_columns,
    add_date_dimensions,
    apply_scd_type_2,
    build_sales_order_transformations,
    create_cdc_incremental_load,
    create_incremental_load_markers,
    deduplicate_customer_records,
    reconcile_fact_dimension,
)
from pyspark.sql import functions as F


def test_full_name_and_status_transformation(raw_customer_df, operational_customer_df):
    expected = (
        raw_customer_df
        .withColumn("expected_full_name", F.trim(F.concat_ws(" ", F.trim(F.col("first_name")), F.trim(F.col("last_name")))))
        .withColumn("expected_status", F.upper(F.trim(F.col("status"))))
        .withColumn("expected_amount", F.round(F.abs(F.col("amount").cast("double")), 2))
        .select("customer_id", "expected_full_name", "expected_status", "expected_amount")
    )

    actual = (
        operational_customer_df
        .select(
            "customer_id",
            F.col("full_name").alias("actual_full_name"),
            F.col("status").alias("actual_status"),
            F.col("amount").alias("actual_amount"),
        )
    )

    comparison = expected.join(actual, "customer_id", "inner")
    mismatches = comparison.filter(
        (~F.col("expected_full_name").eqNullSafe(F.col("actual_full_name")))
        | (~F.col("expected_status").eqNullSafe(F.col("actual_status")))
        | (~F.col("expected_amount").eqNullSafe(F.col("actual_amount")))
    )

    assert mismatches.count() == 0, f"Found transformation mismatches: {mismatches.collect()}"


def test_advanced_real_time_transformations(operational_customer_df):
    first_customer = operational_customer_df.filter(F.col("customer_id") == "C001").first()

    assert first_customer["full_name"] == "john doe"
    assert first_customer["email_domain"] == "company.com"
    assert first_customer["has_valid_email"] is True
    assert first_customer["customer_tier"] == "BRONZE"
    assert first_customer["risk_flag"] == "LOW"
    assert first_customer["customer_segment"] == "REGULAR"
    assert first_customer["signup_month"] == "2024-01"
    assert first_customer["customer_age_days"] >= 0


def test_production_style_transformations(raw_customer_df):
    dedup_df = deduplicate_customer_records(raw_customer_df, ["customer_id"])
    audit_df = add_audit_columns(dedup_df, batch_id="BATCH_2024_09", source_system="CRM")
    date_df = add_date_dimensions(audit_df, "signup_date")
    incremental_df = create_incremental_load_markers(date_df, "2024-09-25")

    assert dedup_df.count() == raw_customer_df.count()
    assert "batch_id" in incremental_df.columns
    assert "load_date" in incremental_df.columns
    assert "signup_month" in incremental_df.columns
    assert "is_weekend" in incremental_df.columns


def test_active_flag_for_business_rules(operational_customer_df):
    invalid_active_rows = operational_customer_df.filter(
        F.col("status").isin("ACTIVE", "PENDING", "NEW")
        & (F.col("is_active") == False)
    )
    assert invalid_active_rows.count() == 0


def test_status_values_are_standardized(operational_customer_df):
    invalid_rows = operational_customer_df.filter(
        ~F.col("status").isin("ACTIVE", "INACTIVE", "PENDING", "NEW")
    )
    assert invalid_rows.count() == 0


def test_cdc_incremental_load_filters_latest_rows(spark):
    current_rows = [
        ("C001", "2024-01-01", "ACTIVE", "2024-09-01"),
        ("C002", "2024-02-02", "INACTIVE", "2024-09-05"),
    ]
    incoming_rows = [
        ("C001", "2024-01-01", "ACTIVE", "2024-09-10"),
        ("C003", "2024-03-03", "PENDING", "2024-09-07"),
    ]
    current_df = spark.createDataFrame(current_rows, ["customer_id", "signup_date", "status", "updated_at"])
    incoming_df = spark.createDataFrame(incoming_rows, ["customer_id", "signup_date", "status", "updated_at"])

    delta_df = create_cdc_incremental_load(current_df, incoming_df, key_col="customer_id", timestamp_col="updated_at")

    assert delta_df.filter(F.col("customer_id") == "C003").count() == 1
    assert delta_df.filter(F.col("customer_id") == "C001").count() == 1
    assert delta_df.filter(F.col("row_action") == "UPSERT").count() == 2


def test_scd_type_2_history_generation(spark):
    dim_df = spark.createDataFrame(
        [("D1", "A", "2023-01-01", "2023-12-31", True, 1), ("D2", "B", "2023-02-01", None, True, 1)],
        ["customer_id", "status", "valid_from", "valid_to", "is_current", "version_no"],
    )
    incoming_df = spark.createDataFrame(
        [("D1", "B", "2024-01-01"), ("D2", "B", "2024-02-01")],
        ["customer_id", "status", "updated_at"],
    )

    scd_df = apply_scd_type_2(dim_df, incoming_df, business_key="customer_id", change_col="status", effective_start_col="valid_from", effective_end_col="valid_to", is_current_col="is_current", version_col="version_no")

    assert scd_df.filter(F.col("customer_id") == "D1").count() >= 2
    assert scd_df.filter((F.col("customer_id") == "D1") & (F.col("is_current") == True)).count() == 1
    assert scd_df.filter((F.col("customer_id") == "D1") & (F.col("version_no") == 2)).count() == 1


def test_fact_dimension_reconciliation(spark):
    dim_df = spark.createDataFrame([("D1",), ("D2",)], ["customer_id"])
    fact_df = spark.createDataFrame([("D1", 100.0), ("D3", 50.0), ("D2", 25.0)], ["customer_id", "amount"])

    invalid_df = reconcile_fact_dimension(fact_df, dim_df, fact_key="customer_id", dim_key="customer_id", metric_col="amount")

    assert invalid_df.count() == 1
    assert invalid_df.first()["customer_id"] == "D3"


def test_order_transformations(spark):
    order_df = spark.createDataFrame(
        [
            ("ORD001", "C001", "2024-09-10", "PENDING", 100.0, 15.0),
            ("ORD002", "C002", "2024-09-12", "shipped", -10.0, 5.0),
            ("ORD003", "C003", "2024-09-16", "DELIVERED", 200.0, 25.0),
        ],
        ["order_id", "customer_id", "order_date", "order_status", "gross_amount", "discount_amount"],
    )

    sales_df = build_sales_order_transformations(order_df)

    assert "net_amount" in sales_df.columns
    assert "order_month" in sales_df.columns
    assert "status_flag" in sales_df.columns
    assert sales_df.filter(F.col("customer_id") == "C002").first()["net_amount"] == 5.0
    assert sales_df.filter(F.col("order_status") == "DELIVERED").count() == 1
