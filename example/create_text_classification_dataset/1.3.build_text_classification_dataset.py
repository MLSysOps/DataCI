import random
import subprocess
import inspect
from pathlib import Path

from dataci.models import Stage

FILE_WORK_DIR = Path(__file__).parent.resolve()

print('=' * 80)
print('Step 1: Build train dataset v1')
print('=' * 80)

from build_text_dataset import main

source_code = inspect.getsource(main)
print('workflow source code:')
print(source_code)

print('Test the build text dataset workflow:')
# Set seed for reproducibility
random.seed(42)

dataset_identifier = main()
print(dataset_identifier)

# Data Quality Check
data_qc = Stage.get('official.data_qc@latest')
try:
    data_qc(dataset_identifier=dataset_identifier)
    print('Data Quality Check Passed!')
except Exception as e:
    print('Data Quality Check Failed!', e)

# Run benchmarking
print('=' * 80)
print('Step 2: Dataset benchmarking')
print('=' * 80)

dc_benchmark = Stage.get('official.dc_bench@latest')
benchmark_result = dc_benchmark(dataset_identifier=dataset_identifier)

# Publish data workflow
print('=' * 80)
print('Step 4: Publish data workflow')
print('=' * 80)
command = f'dataci workflow publish {FILE_WORK_DIR}/build_text_dataset.py main'
print("Run command: ", command)
subprocess.run(command, shell=True, check=True)

print('=' * 80)
print('Step 5: Publish first version of text dataset')
print('=' * 80)
command = f'dataci dataset publish {dataset_identifier}'
print("Run command: ", command)
subprocess.run(command, shell=True, check=True)
