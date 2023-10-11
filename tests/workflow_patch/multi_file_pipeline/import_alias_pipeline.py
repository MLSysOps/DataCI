#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 22, 2023
"""
from datetime import datetime
from dataci.plugins.decorators import dag, Dataset


# from stage_mod import func_name as func_name_alias
from step03_used_stage import used_stage as step03_stage
# import stage_mode, use as: stage_mod.func_name
import step04_used_stage_w_util as step04_mod

import step05_multi_file_stage_w_util.multi_file_stage_w_util as step05_mod
import step06_multi_file_stage.multi_file_stage as step06_mod

from utils import common_util


@dag(
    start_date=datetime(2020, 7, 30), schedule=None,
)
def import_alias_pipeline():
    common_util()
    raw_dataset_train = Dataset.get('test_yelp_review@latest')
    df = step03_stage(raw_dataset_train)
    df = step04_mod.used_stage_w_util(df)
    df = step05_mod.multi_file_stage_w_util(df)
    df = step06_mod.multi_file_stage(df)
    Dataset(name='test_text_aug', dataset_files=df)


# Build the pipeline
import_alias_dag = import_alias_pipeline()
