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

    input_dataset = context['params'].get('input_data', None)
    df = DataFrameHook.read(dataset_identifier=input_dataset, **context)
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
    name='build_text_dataset',
    params={'input_data': 'text_cls_raw@1'},
)
def main():
    data_read >> text_aug >> data_save


if __name__ == '__main__':
    main.publish()
