#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 22, 2023
"""
from datetime import datetime
from dataci.plugins.decorators import dag, Dataset


from step03_used_stage import used_stage
from step04_used_stage_w_util import used_stage_w_util
from step05_multi_file_stage_w_util.multi_file_stage_w_util import multi_file_stage_w_util
from step06_multi_file_stage.multi_file_stage import multi_file_stage
from utils import common_util


@dag(
    start_date=datetime(2020, 7, 30), schedule=None,
)
def normal_import_pipeline():
    common_util()
    raw_dataset_train = Dataset.get('yelp_review@latest')
    df = used_stage(raw_dataset_train)
    df = used_stage_w_util(df)
    df = multi_file_stage_w_util(df)
    df = multi_file_stage(df)
    Dataset(name='text_aug', dataset_files=df)


# Build the pipeline
normal_import_dag = normal_import_pipeline()
