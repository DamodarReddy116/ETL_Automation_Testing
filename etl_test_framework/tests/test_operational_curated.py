from pyspark.sql import functions as F


def test_curated_total_amount_matches_operational(operational_customer_df, curated_customer_df):
    expected = (
        operational_customer_df.groupBy("customer_id")
        .agg(F.sum("amount").alias("expected_total_amount"))
    )

    actual = curated_customer_df.select("customer_id", "total_amount")

    comparison = expected.join(actual, "customer_id", "full_outer")
    mismatches = comparison.filter(
        ~F.col("expected_total_amount").eqNullSafe(F.col("total_amount"))
    )

    assert mismatches.count() == 0, f"Curated amount mismatch: {mismatches.collect()}"


def test_customer_tier_logic(curated_customer_df):
    invalid_tier_rows = curated_customer_df.filter(
        ~F.col("customer_tier").isin("BRONZE", "SILVER", "GOLD")
    )
    assert invalid_tier_rows.count() == 0
