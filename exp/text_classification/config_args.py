#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 21, 2023
"""
import os

from dataci.plugins.decorators import stage


@stage
def config_train_args(train_dataset_path, val_dataset_path):
    return [f'--train_dataset={train_dataset_path}', f'--test_dataset={val_dataset_path}']


@stage
def config_predict_args(train_output_path, test_dataset_path):
    return [f'--test_dataset={test_dataset_path}', f'--model_name={os.path.join(train_output_path, "model")}']
