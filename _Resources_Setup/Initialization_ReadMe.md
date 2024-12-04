# Initialization Steps to create DE Demo

### 1-Upload Notebooks from the Repo

### 2-Recreate Job from CLI
- Open terminal from Local and validate if v0.221.1 is available: `databricks --version`
- Connect to the workspace: `databricks configure --token`
- Insert the host: e.g. https://e2-demo-field-eng.cloud.databricks.com/
- Insert a PAT
- Create a Serverless Job based on json configurations: `databricks jobs create --json @"/Users/gabriele.albini/Downloads/_files/DE_Job.json"`