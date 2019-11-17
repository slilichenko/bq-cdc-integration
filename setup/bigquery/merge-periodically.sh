#!/usr/bin/env bash

set -e

while true; do
  cat merge-session.sql | bq query --use_legacy_sql=false
  sleep 120
done