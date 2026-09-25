-- Databricks SQL script to create the raw, operational, and curated tables.
-- Update the catalog/schema names as needed for your workspace.

CREATE CATALOG IF NOT EXISTS etl_demo;
CREATE SCHEMA IF NOT EXISTS etl_demo.bronze;
CREATE SCHEMA IF NOT EXISTS etl_demo.silver;
CREATE SCHEMA IF NOT EXISTS etl_demo.gold;

CREATE TABLE IF NOT EXISTS etl_demo.bronze.raw_customer_data (
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  status STRING,
  signup_date DATE,
  amount DOUBLE,
  updated_at TIMESTAMP,
  region STRING,
  source_system STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS etl_demo.silver.operational_customer_data (
  customer_id STRING,
  full_name STRING,
  email_domain STRING,
  has_valid_email BOOLEAN,
  status STRING,
  signup_date DATE,
  amount DOUBLE,
  customer_tier STRING,
  risk_flag STRING,
  customer_segment STRING,
  is_active BOOLEAN,
  is_high_value BOOLEAN,
  signup_month STRING,
  customer_age_days INT,
  updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS etl_demo.gold.curated_customer_metrics (
  customer_id STRING,
  total_amount DOUBLE,
  avg_amount DOUBLE,
  latest_signup_date DATE,
  first_signup_date DATE,
  total_records BIGINT,
  active_record_count BIGINT,
  high_value_record_count BIGINT,
  customer_tier STRING,
  review_flag INT
) USING DELTA;

-- Optional example of a data-load pattern:
-- COPY INTO etl_demo.bronze.raw_customer_data
-- FROM '/Volumes/.../customer_source.csv'
-- FILEFORMAT = CSV
-- FORMAT_OPTIONS ('header' = 'true');
