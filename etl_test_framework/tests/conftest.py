import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from framework.transformations import apply_advanced_customer_transformations, build_curated_customer_metrics


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("etl-framework-tests").getOrCreate()


@pytest.fixture(scope="module")
def source_customer_df(spark):
    rows = [
        {
            "customer_id": "C001",
            "first_name": " john ",
            "last_name": "doe",
            "email": "JOHN.DOE@COMPANY.COM",
            "status": " active ",
            "signup_date": "2024-01-15",
            "amount": "120.50",
        },
        {
            "customer_id": "C002",
            "first_name": "Mary",
            "last_name": "Jones",
            "email": "mary.jones@company.com",
            "status": "inactive",
            "signup_date": "2024-02-05",
            "amount": "-80.00",
        },
        {
            "customer_id": "C003",
            "first_name": "Alex",
            "last_name": "Smith",
            "email": "alex.smith@company.com",
            "status": "pending",
            "signup_date": "2024-05-10",
            "amount": "0.00",
        },
    ]
    return spark.createDataFrame(rows)


@pytest.fixture(scope="module")
def raw_customer_df(source_customer_df):
    return source_customer_df


@pytest.fixture(scope="module")
def operational_customer_df(raw_customer_df):
    return apply_advanced_customer_transformations(raw_customer_df)


@pytest.fixture(scope="module")
def curated_customer_df(operational_customer_df):
    return build_curated_customer_metrics(operational_customer_df)
