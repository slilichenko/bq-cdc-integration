#!/bin/bash

set -e

DATASET_ID=$1

bq rm -f -t ${DATASET_ID}.source_session
