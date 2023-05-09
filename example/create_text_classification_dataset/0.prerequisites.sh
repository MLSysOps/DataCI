#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd ${SCRIPT_DIR}/../..
echo "Initialize DataCI"
python dataci/command/init.py init

echo "Connect to Cloud Services"
python dataci/command/connect.py s3 -a
