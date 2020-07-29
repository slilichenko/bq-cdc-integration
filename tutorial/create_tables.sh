#!/usr/bin/env bash

DATASET=cdc_tutorial

bq mk ${DATASET}

bq mk -t ${DATASET}.session_main id:string,username:string,change_id:integer
bq mk -t ${DATASET}.session_delta id:string,username:string,change_id:integer,change_type:string
