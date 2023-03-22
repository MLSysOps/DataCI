#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd ${SCRIPT_DIR}/../..
echo "Initialize DataCI"
python dataci/command/init.py -f

echo "Download Sample Raw Data"
# saved at data/text_raw/
mkdir -p data
rm -r data/*
cp -r dataset/text_cls_v1 data/text_raw
