#!/bin/bash

set -e

PROJECT_ID=$1
BQ_DATASET=$2
FINAL_DEF_FILE=session-def-final.json

cat session-def.json | sed s/PROJECT_ID/${PROJECT_ID}/ > ${FINAL_DEF_FILE}

bq mk --external_table_definition=${FINAL_DEF_FILE} ${BQ_DATASET}.source_session

rm ${FINAL_DEF_FILE}
