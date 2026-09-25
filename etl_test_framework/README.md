# ETL Test Automation Framework

This project demonstrates a reusable PySpark + PyTest framework for validating ETL data as it moves across source, raw, operational, and curated layers.

## Folder structure

```text
etl_test_framework/
├── config/
│   ├── __init__.py
│   └── validation_config.py
├── framework/
│   ├── __init__.py
│   ├── data_reader.py
│   ├── validations.py
│   ├── reconciliation.py
│   └── transformations.py
├── tests/
│   ├── conftest.py
│   ├── test_source_raw.py
│   ├── test_raw_operational.py
│   └── test_operational_curated.py
├── pytest.ini
├── README.md
└── requirements.txt
```

## Typical validation flow

1. Read source CSV/TXT files
2. Reconcile source and raw records
3. Validate mandatory columns, nulls, and duplicates
4. Apply business transformations
5. Compare expected values against operational outputs
6. Aggregate into curated metrics and validate KPI logic

## Run tests

```bash
cd etl_test_framework
pytest -q
```

## Real-world notes

- Keep paths, keys, and allowed values in configuration files.
- Use Spark joins and aggregates instead of collect() for large datasets.
- Add batch_id or load_date filters for incremental ETL validation.
- Integrate the suite into Databricks jobs or CI/CD pipelines.
