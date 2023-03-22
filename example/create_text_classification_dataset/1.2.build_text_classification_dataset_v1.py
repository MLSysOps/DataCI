import subprocess
from pathlib import Path

import augly.text as txtaugs
import torch

from dataci.pipeline import Pipeline, stage

FILE_WORK_DIR = Path(__file__).parent.resolve()

print('=' * 80)
print('Step 1: Build train dataset v1')
print('=' * 80)


# Data processing: text augmentation
@stage(inputs='text_raw_train', outputs='text_aug.csv')
def text_augmentation(inputs):
    transform = txtaugs.InsertPunctuationChars(
        granularity="all",
        cadence=5.0,
        vary_chars=True,
    )
    inputs['product_name'] = inputs['product_name'].map(transform)
    return inputs


# Define data pipeline
train_data_pipeline = Pipeline(name='train_data_pipeline', stages=[text_augmentation])
train_data_pipeline.build()

print('Debug/test run the train data pipeline:')
train_data_pipe_run = train_data_pipeline()

# Run benchmarking
print('=' * 80)
print('Step 2: Benchmark the performance of the built classification dataset')
print('=' * 80)
print('Train bert-base-uncased using the text classification dataset...')
print('Checking CUDA availability...')
if torch.cuda.is_available():
    print(f'Found CUDA: {torch.device("cuda")}')
    other_args = ['-b32']
else:
    print(
        'Not found CUDA. I will adjust the benchmarking parameters for you:\n'
        '- Less batch size\n'
        '- Train a few steps instead of the whole iteration\n'
        '- Evaluate and test with few steps instead of the whole iteration\n'
        '- Smaller log printing frequency'
    )
    other_args = ['-b4', '--max_train_steps_per_epoch=20', '--max_val_steps_per_epoch=20', '--print_freq=5']
cmd = [
          'python', str(FILE_WORK_DIR / 'train.py'),
          f'--train_dataset={FILE_WORK_DIR / "train_data_pipeline/latest/runs/1/feat/text_aug.csv"}',
          f'--test_dataset={FILE_WORK_DIR / "../../data/text_raw/val.csv"}',
      ] + other_args
print('Benchmarking with command:', ' '.join(cmd))
subprocess.call(cmd)
print('Finish benchmarking!')

# Publish
print('=' * 80)
print('Step 3: Save train data pipeline')
print('=' * 80)
train_data_pipeline.publish()

print('=' * 80)
print('Step 4: Publish first version of text dataset (i.e., train_data_pipeline:text_aug.csv):')
print('=' * 80)
train_data_pipeline()
