#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "${SCRIPT_DIR}/../../"

echo "================================================================================"
echo "Step 1: Download text_raw_v2"
echo "================================================================================"
cp -rf dataset/text_cls_v2 data/text_raw_v2/

echo "================================================================================"
echo "Step 2: Publish text_raw v2"
echo "================================================================================"
python dataci/command/dataset.py publish -n text_raw_train data/text_raw_v2/train.csv

echo "================================================================================"
echo "Step 3: We can easily update our text classification dataset"
echo "================================================================================"
python dataci/command/dataset.py update -n train_data_pipeline:text_aug
