#!/usr/bin/env bash

set -e

(cd terraform; terraform init; terraform apply )

