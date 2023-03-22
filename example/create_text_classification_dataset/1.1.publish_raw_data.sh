#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "${SCRIPT_DIR}/../../"

python dataci/command/dataset.py publish -n text_raw_train data/text_raw/train.csv
python dataci/command/dataset.py publish -n text_raw_val data/text_raw/val.csv
