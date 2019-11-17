#!/usr/bin/env bash

rm -f 'sync.stop'

java -jar target/data-generator-1.0-SNAPSHOT-shaded.jar "$@" &
