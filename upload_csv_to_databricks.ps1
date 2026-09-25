$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$source = Join-Path $root "data\customer_source.csv"
$dest = "/FileStore/etl_demo/customer_source.csv"

if (-not (Test-Path $source)) {
    throw "Source CSV not found at $source. Run generate_customer_data.py first."
}

Write-Host "Uploading $source to Databricks DBFS at $dest"
& databricks fs mkdirs dbfs:/FileStore/etl_demo
& databricks fs cp $source $dest --overwrite

Write-Host "Upload complete. You can now run databricks_load_tables.py in a Databricks notebook."
