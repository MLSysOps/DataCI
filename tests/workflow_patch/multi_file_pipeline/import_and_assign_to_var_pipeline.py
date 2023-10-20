#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 22, 2023
"""
from datetime import datetime
from dataci.plugins.decorators import dag, Dataset


import step03_used_stage
import step04_used_stage_w_util
import step05_multi_file_stage_w_util.multi_file_stage_w_util
import step06_multi_file_stage.multi_file_stage
from utils import common_util

import types

packages = types.ModuleType('packages')
packages.step1_stage = step03_used_stage.used_stage
packages.step2_stage = step04_used_stage_w_util.used_stage_w_util
packages.step3_stage = step05_multi_file_stage_w_util.multi_file_stage_w_util.multi_file_stage_w_util
packages.step4_stage = step06_multi_file_stage.multi_file_stage.multi_file_stage


@dag(
    start_date=datetime(2020, 7, 30), schedule=None,
)
def import_and_assign_to_var_pipeline():
    common_util()
    raw_dataset_train = Dataset.get('yelp_review@latest')
    df = packages.step1_stage(raw_dataset_train)
    df = packages.step2_stage(df)
    df = packages.step3_stage(df)
    df = packages.step4_stage(df)
    Dataset(name='text_aug', dataset_files=df)


# Build the pipeline
import_and_assign_to_var_dag = import_and_assign_to_var_pipeline()
