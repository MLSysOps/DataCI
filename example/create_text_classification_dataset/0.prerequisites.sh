#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd ${SCRIPT_DIR}/../..
echo "Initialize DataCI"
python dataci/command/init.py init

echo "Connect to Cloud Services"
#read -p "AWS S3 Access Key ID: " AWS_ACCESS_KEY_ID
python dataci/command/connect.py s3 -k AKIAWCBW2AMEL7Q5YY2O -p

echo "Create a new workspace"
python dataci/command/workspace.py use testspace

echo "Load some common stages and workflows"
python dataci/command/stage.py publish action_hub.data_qc:data_quality_check
python dataci/command/stage.py publish action_hub.benchmark.dc_bench:data_centric_benchmark
python dataci/command/workflow.py publish action_hub.ci_cd_trigger:trigger_ci_cd
