# ETL_Automation_Testing

## Connect to Databricks

This project uses the Databricks SDK and environment variables for authentication. Do not commit a real token.

1. Install the dependency:

	```powershell
	python -m pip install -r requirements.txt
	```

2. Set the workspace URL and token in the current PowerShell session:

	```powershell
	$env:DATABRICKS_HOST = "https://<workspace>.cloud.databricks.com"
	$env:DATABRICKS_TOKEN = "<your-token>"
	```

3. Test the connection:

	```powershell
	python connect_databricks.py
	```

The script prints the authenticated Databricks username when the connection succeeds. The environment variables apply only to the current terminal session.