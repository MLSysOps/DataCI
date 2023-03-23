#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 22, 2023
"""
import subprocess
from pathlib import Path

import augly.text as txtaugs

from dataci.pipeline import stage, Pipeline

FILE_WORK_DIR = Path(__file__).parent.resolve()

print('=' * 80)
print('Step 1: Write a second version train data pipeline')
print('=' * 80)


@stage(inputs='text_raw_train', outputs='text_aug.csv')
def text_augmentation(inputs):
    transform = txtaugs.Compose(
        [
            txtaugs.InsertWhitespaceChars(p=0.5),
            txtaugs.InsertPunctuationChars(
                granularity="all",
                cadence=5.0,
                vary_chars=True,
            )
        ]
    )
    inputs['product_name'] = inputs['product_name'].map(transform)
    return inputs


train_data_pipeline_v2 = Pipeline(name='train_data_pipeline', stages=[text_augmentation])

print('=' * 80)
print('Step 2: Test the pipeline v2 and publish')
print('=' * 80)
print('Run train data pipeline v2')
train_data_pipeline_v2()
print('Save train data pipeline v2')
train_data_pipeline_v2.publish()

print('Check published pipelines:')
cmd = ['python', str(FILE_WORK_DIR / '../../dataci/command/pipeline.py'), 'ls', 'train_data_pipeline']
print(f'Executing command: {" ".join(cmd)}')
subprocess.call(cmd)

print('=' * 80)
print('Step 3: Publish text classification  dataset v2 (train_data_pipeline:text_aug v2)')
print('=' * 80)
train_data_pipeline_v2()
