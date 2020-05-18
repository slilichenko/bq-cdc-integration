# BigQuery - Change Data Capture Demo

## Overview
This project uses a simple data generator to simulate web site purchasing behavior. 
Source data is captured in a BigTable and the changes are pushed via streaming inserts into BigQuery.
The source code provides several scripts that help monitor of the data replication process and data availability in various tables.

## Setup
**Note:** the demo uses BigTable and BigQuery and you may incur changes. To avoid or minimize the charges make sure to shut down the data generation script and clean up the environment at the end.

Decide which GCP project you would like to run this demo in. You need to have sufficient privileges in that project to create BigQuery dataset and tables and a BigTable cluster and a table.
```
export TF_VAR_project_id=<project-id>
```
To create the environment for the demo:

```
find . -name '*.sh' -exec chmod +x {} \;
cd setup
./create-all.sh
```
At the end of the successful setup you will have created:
1. BigTable cluster `cdc-demo`
1. Table `session` in that cluster
1. BigQuery dataset `cdc_demo`
1. Tables `session_main` and `session_delta`
1. View `session_source_v` over the BigTable's `session` table (as a federated data source)
1. View `session_latest_v` 

## Cleanup
```
cd setup
./remove-all.sh
```