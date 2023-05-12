#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "${SCRIPT_DIR}/../../" || exit 1

dataci dataset save -n text_cls_raw s3://dataci-shared/text_cls_v1/train.csv
dataci dataset publish text_cls_raw@1b
