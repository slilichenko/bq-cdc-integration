#!/usr/bin/env bash

DATASET=cdc_tutorial

bq mk ${DATASET}

bq mk -t ${DATASET}.session_main id:string,username:string,sync_id:integer
bq mk -t ${DATASET}.session_delta id:string,username:string,sync_id:integer,sync_type:string
