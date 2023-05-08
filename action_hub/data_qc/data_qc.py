#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 08, 2023
"""
from dataci.decorators.stage import stage
from dataci.hook.df_hook import DataFrameHook


@stage(name='official.data_qc')
def data_quality_check(dataset_identifier, **context):
    """Check dataset quality."""
    df = DataFrameHook.read(dataset_identifier, **context)
    # If data have duplicated rows, raise error
    if df.duplicated().any():
        raise ValueError('Dataset have duplicated rows.')
    # If data have null values, raise error
    if df.isna().any().any():
        raise ValueError('Dataset have null values.')

    return dataset_identifier


if __name__ == '__main__':
    data_quality_check.publish()
