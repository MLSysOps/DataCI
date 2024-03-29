#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "${SCRIPT_DIR}/../.." || exit 1
echo "Initialize DataCI"
dataci init

echo "Connect to Cloud Services"
read -r -p "AWS S3 Access Key ID: " AWS_ACCESS_KEY_ID
dataci connect s3 -k "${AWS_ACCESS_KEY_ID}"

echo "Create a new workspace"
dataci workspace use testspace

echo "Load some common stages and workflows"
dataci stage publish action_hub/data_qc.py data_quality_check
dataci stage publish action_hub/benchmark/dc_bench.py data_centric_benchmark
dataci stage publish action_hub/execute_workflow.py execute_workflow

echo "Save Dataset v1"
dataci dataset save -n text_cls_raw s3://dataci-shared/text_cls_v1/train.csv
dataci dataset publish text_cls_raw@1b
