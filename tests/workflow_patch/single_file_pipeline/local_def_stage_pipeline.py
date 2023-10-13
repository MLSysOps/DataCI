#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 22, 2023
"""
import augly.text as textaugs

from dataci.plugins.decorators import stage


def common_util_function():
    # do nothing here
    pass


from datetime import datetime
from dataci.plugins.decorators import dag, Dataset


@dag(
    start_date=datetime(2020, 7, 30), schedule=None,
)
def text_process_ci():
    @stage
    def unused_stage(df):
        common_util_function()
        return df

    @stage
    def step0_standalone_stage(df):
        return df.sample(frac=1)

    @stage
    def step1_intra_deps_stage(df):
        common_util_function()
        aug_function = textaugs.ReplaceSimilarUnicodeChars()
        df['text'] = aug_function(df['text'].tolist())
        return df

    raw_dataset_train = Dataset.get('yelp_review@latest')
    df = step0_standalone_stage(raw_dataset_train)
    step1_intra_deps_stage(df)


# Build the pipeline
text_process_ci_pipeline = text_process_ci()
