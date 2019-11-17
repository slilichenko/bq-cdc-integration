#!/usr/bin/env bash

set -e

while true; do
  cat merge-session-status.sql | bq query --use_legacy_sql=false
  sleep 15
done