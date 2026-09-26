# ETL Test Automation Framework

Reusable PySpark and pytest framework for validating customer ETL pipelines across source, raw, operational, and curated layers. The suite runs locally with a Spark session and produces Allure results for test reporting.

## What This Framework Tests

The framework validates both data quality and business transformations:

- Source and raw datasets are not empty.
- Record counts match between source and raw layers.
- Required columns exist and primary keys are not duplicated.
- Source and target keys reconcile in both directions.
- Text, status, email, date, and amount fields are standardized.
- Customer tiers, risk flags, activity flags, and customer segments follow business rules.
- Incremental-load markers and CDC rows are generated correctly.
- SCD Type 2 history creates one current version and closes changed versions.
- Fact rows without a matching dimension key are identified.
- Sales-order amounts, statuses, months, and value buckets are derived correctly.
- Curated customer totals and tier values match operational data.

## Data Flow

```text
Source file/table
	|
	v
Raw layer: ingestion and basic standardization
	|
	v
Operational layer: cleaned, enriched, and business-ready records
	|
	v
Curated layer: customer-level metrics and reporting KPIs
```

The automated tests use a small in-memory customer DataFrame so they are repeatable and do not require Databricks data. The repository also contains a generated 100,000-row source file for integration-style Databricks loading.

## Project Structure

```text
etl_test_framework/
├── config/
│   └── validation_config.py       # Keys, required columns, statuses, thresholds
├── framework/
│   ├── data_reader.py             # CSV, TXT, table, and SQL readers
│   ├── validations.py             # Reusable data-quality assertions
│   ├── reconciliation.py          # Key and source-target reconciliation
│   └── transformations.py         # Customer, CDC, SCD, fact, and order logic
├── tests/
│   ├── conftest.py                # Spark session and test DataFrame fixtures
│   ├── test_source_raw.py         # Source/raw completeness and reconciliation
│   ├── test_raw_operational.py    # Operational and advanced transformations
│   └── test_operational_curated.py # Curated metric and KPI checks
├── pytest.ini                     # pytest and Allure configuration
├── requirements.txt               # Framework dependencies
└── README.md
```

## Prerequisites

- Python 3.10 or a compatible Python version supported by the installed PySpark release.
- Java available on `PATH` for local Spark.
- A virtual environment is recommended.
- Allure CLI is required only when generating the HTML report.

From the repository root in PowerShell:

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The repository already includes a `.venv` in the current setup. Do not upload that folder to Databricks; it contains local Windows dependencies and is not a Databricks runtime environment.

## Run the Tests

Run from the repository root:

```powershell
& ".\.venv\Scripts\python.exe" -m pytest ".\etl_test_framework" --clean-alluredir
```

Or, after activating the environment:

```powershell
cd etl_test_framework
pytest --clean-alluredir
```

The `pytest.ini` file adds the package to `PYTHONPATH`, enables concise output, and writes results to `allure-results`:

```ini
[pytest]
pythonpath = .
addopts = --alluredir=allure-results -ra -q
```

To run one test module or one test:

```powershell
pytest .\etl_test_framework\tests\test_raw_operational.py -q
pytest .\etl_test_framework\tests\test_raw_operational.py -k cdc -q
```

## Generate the Allure Report

After pytest finishes, generate the HTML report from the results:

```powershell
.\allure_report.ps1
```

The script expects the local Allure executable at:

```text
tools/allure/allure-2.28.0/bin/allure.bat
```

The generated report is written to `allure-report`. Open `allure-report/index.html` in a browser. The report should be regenerated after each test run so it contains only the latest results.

## Framework APIs

### `DataReader`

`framework/data_reader.py` provides Spark readers for:

- CSV files with configurable delimiter, header, and schema inference.
- Delimited text files.
- Registered Spark tables.
- Arbitrary Spark SQL queries.

### `DataValidator`

`framework/validations.py` provides assertions for:

- Non-empty datasets.
- Source and target record counts.
- Null checks on selected columns.
- Duplicate business keys.
- Required columns.
- Allowed values such as customer statuses.
- Expected Spark data types.

### `Reconciliation`

`framework/reconciliation.py` compares distinct business keys using left-anti joins. It detects both source keys missing from the target and unexpected target keys.

### `transformations.py`

The transformation module includes:

- Customer standardization and null remediation.
- Customer amount, status, email, date, tier, risk, and segment rules.
- Audit columns, date dimensions, and incremental-load markers.
- Reference-data enrichment.
- Curated customer metrics.
- CDC incremental filtering using update timestamps.
- SCD Type 2 history generation.
- Fact-to-dimension referential-integrity checks.
- Sales-order normalization and value buckets.

## Configuration

Edit `config/validation_config.py` when business rules change. It contains:

- Source path and format.
- Raw, operational, and curated table names.
- Primary key columns.
- Mandatory columns.
- Allowed status values.
- Expected advanced fields.
- Customer tier and risk thresholds.

Keep environment-specific paths and credentials outside the test code. Never commit Databricks tokens or passwords.

## Databricks Integration

The repository root contains these Databricks assets:

```text
databricks_load_tables.py       # Spark/dbutils loader for notebook execution
databricks_create_tables.sql    # Raw, operational, and curated table DDL
databricks.yml                  # Databricks bundle configuration
data/customer_source.csv        # Generated source data
```

The Databricks data model is:

```text
etl_demo.bronze.raw_customer_data
etl_demo.silver.operational_customer_data
etl_demo.gold.curated_customer_metrics
```

The Python loader expects a Databricks notebook or cluster context with `spark` and `dbutils`. In workspaces where public DBFS is disabled, upload the source file to a Unity Catalog volume and update the input path accordingly. The current validated SQL execution used:

```text
/Volumes/etl_demo/bronze/source_files/customer_source.csv
```

The operational output includes the `email` column. If tables are created manually, ensure their schema matches the DataFrame exactly or use an explicit schema overwrite/recreation during development.

## Adding a New Test

1. Add or update a fixture in `tests/conftest.py` if the scenario needs reusable Spark data.
2. Add a focused test to the layer-specific test module.
3. Use `DataValidator` and `Reconciliation` for common checks instead of duplicating assertions.
4. Add transformation logic to `framework/transformations.py` when it is reusable production behavior.
5. Run the focused test first, then the complete suite.
6. Regenerate the Allure report.

Example:

```python
def test_new_business_rule(operational_customer_df):
    invalid_rows = operational_customer_df.filter("customer_tier IS NULL")
    assert invalid_rows.count() == 0
```

## Scaling and Maintenance Notes

- Prefer Spark expressions, joins, and aggregations over collecting large datasets to the driver.
- Keep `collect()`-based logic limited to small stateful examples such as the current SCD test helper.
- Add `batch_id`, `load_date`, or watermark filters for incremental validation.
- Use deterministic fixtures for unit tests and representative files for integration tests.
- Keep source, operational, and curated schemas versioned with the pipeline.
- Run the suite in CI/CD or as a Databricks job after deploying ETL changes.
