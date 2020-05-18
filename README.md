# BigQuery - Change Data Capture Demo

## Overview
This project uses a simple data generator to simulate web site purchasing behavior. 
Source data is captured in a BigTable and the changes are pushed via streaming inserts into BigQuery.
The source code provides several scripts that help monitor of the data replication process and data availability in various tables.

## Setup
**Note:** the demo uses BigTable and BigQuery and you may incur changes. To avoid or minimize the charges make sure to shut down the data generation script and clean up the environment at the end.

To create the environment for the demo:

`cd setup
chmod +x *.sh
./create-all.sh
`

## Cleanup
