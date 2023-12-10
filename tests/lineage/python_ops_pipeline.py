#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Dec 10, 2023
"""
from datetime import datetime
from dataci.plugins.decorators import dag, Dataset, stage


@stage
def task1(df):
    return df


@stage
def task2_0(df):
    return df


@stage
def task2_1(df):
    return df


@stage
def task3(df1, df2):
    import pandas as pd

    return pd.concat([df1, df2])


@dag(
    start_date=datetime(2020, 7, 30), schedule=None,
)
def python_ops_pipeline():
    raw_dataset_train = Dataset.get('test.yelp_review_test@latest')
    dataset1 = Dataset(name='test.task1_out', dataset_files=task1(raw_dataset_train))
    dataset2_0 = Dataset(name='test.task2_0_out', dataset_files=task2_0(dataset1))
    dataset2_1 = Dataset(name='test.task2_1_out', dataset_files=task2_1(dataset1))
    dataset3 = Dataset(name='test.task3_out', dataset_files=task3(dataset2_0, dataset2_1))


# Build the pipeline
python_ops_dag = python_ops_pipeline()
