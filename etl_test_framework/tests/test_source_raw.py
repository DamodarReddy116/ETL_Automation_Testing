from config.validation_config import CUSTOMER_CONFIG
from framework.validations import DataValidator
from framework.reconciliation import Reconciliation


def test_source_not_empty(source_customer_df):
    DataValidator.validate_not_empty(source_customer_df)


def test_source_raw_count_matches(source_customer_df, raw_customer_df):
    DataValidator.validate_record_count(source_customer_df, raw_customer_df)


def test_raw_mandatory_columns_present(raw_customer_df):
    DataValidator.validate_columns(raw_customer_df, CUSTOMER_CONFIG["mandatory_columns"])


def test_raw_no_duplicate_primary_keys(raw_customer_df):
    DataValidator.validate_duplicates(raw_customer_df, CUSTOMER_CONFIG["primary_key"])


def test_source_raw_key_reconciliation(source_customer_df, raw_customer_df):
    Reconciliation.validate_key_reconciliation(
        source_customer_df,
        raw_customer_df,
        CUSTOMER_CONFIG["primary_key"],
    )
