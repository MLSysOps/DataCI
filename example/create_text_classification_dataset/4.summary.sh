#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "${SCRIPT_DIR}/../../"

echo "================================================================================"
echo "Step 1: How many datasets are built?"
echo "================================================================================"
echo "Check all registered dataset"
python dataci/command/dataset.py ls
echo "Compair specific dataset versions (TODO)"

echo "================================================================================"
echo "Step 2: Compair between different dataset versions"
echo "================================================================================"
echo "TODO"

echo "================================================================================"
echo "Step 3: How many pipelines are built?"
echo "================================================================================"
python dataci/command/pipeline.py ls
