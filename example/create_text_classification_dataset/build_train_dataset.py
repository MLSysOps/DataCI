#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 09, 2023
"""
from dataci.decorators.stage import stage
from dataci.decorators.workflow import workflow


@stage()
def data_read(**context):
    from dataci.hooks.df_hook import DataFrameHook

    version = context['params'].get('version', None)
    df = DataFrameHook.read(dataset_identifier=f'text_cls_raw@{version}')
    return df, 'product_name'


@stage()
def text_aug(inputs):
    import augly.text as txtaugs

    df, text_col = inputs

    transform = txtaugs.InsertPunctuationChars(
        granularity="all",
        cadence=5.0,
        vary_chars=True,
    )

    outputs = transform(df[text_col].tolist())
    df[text_col] = outputs
    return df


@stage()
def data_save(inputs, **context):
    from dataci.hooks.df_hook import DataFrameHook

    return DataFrameHook.save(name='text_aug_dataset', df=inputs, **context)


@workflow(
    name='build_train_dataset',
    params={'version': 1},
)
def main():
    data_read >> text_aug >> data_save
