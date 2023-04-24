#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 12, 2023
"""
import augly.text as txtaugs
import unicodedata
from cleantext import clean

from dataci.pipeline import Pipeline, stage
from dataci.pipeline.get import get_pipeline


def test_publish_pipeline():
    def clean_func(text):
        # remove emoji, space, and to lower case
        text = clean(text, to_ascii=False, lower=True, normalize_whitespace=True, no_emoji=True)

        return text

    @stage(inputs='pairwise_raw_train', outputs='text_clean.csv')
    def text_clean(inputs):
        inputs['to_product_name'] = inputs['to_product_name'].map(clean_func)
        return inputs

    @stage(inputs='text_clean.csv', outputs='text_aug.csv')
    def text_augmentation(inputs):
        transform = txtaugs.InsertPunctuationChars(
            granularity="all",
            cadence=5.0,
            vary_chars=True,
        )
        inputs['to_product_name'] = inputs['to_product_name'].map(transform)
        return inputs

    # Define a pipeline
    print('Define a pipeline')
    pipeline = Pipeline('train_data_pipeline', stages=[text_clean, text_augmentation])
    pipeline.build()
    print(pipeline.inputs)
    print(pipeline.outputs[0].dataset_files)
    pipeline.publish()


def test_get_pipeline():
    pipeline = get_pipeline('train_data_pipeline')
    print(pipeline)
    print('Input:')
    print(pipeline.inputs)
    print('Output:')
    print(pipeline.outputs)


def test_run_pipeline():
    pipeline = get_pipeline('train_data_pipeline')
    print(pipeline)
    print('Input:')
    print(pipeline.inputs)
    print('Output:')
    print(pipeline.outputs[0].dataset_files)
    print('Run pipeline...')
    pipeline()


def test_publish_pipeline_v2():
    def clean_func(text):
        # remove emoji, space, and to lower case
        text = clean(text, to_ascii=False, lower=True, normalize_whitespace=True, no_emoji=True)

        # remove accent
        text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

        return text

    @stage(inputs='pairwise_raw_train', outputs='text_clean.csv')
    def text_clean(inputs):
        inputs['to_product_name'] = inputs['to_product_name'].map(clean_func)
        return inputs

    @stage(inputs='text_clean.csv', outputs='text_aug.csv')
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
        inputs['to_product_name'] = inputs['to_product_name'].map(transform)
        return inputs

    # Define a pipeline
    print('Define a pipeline')
    pipeline = Pipeline('train_data_pipeline', stages=[text_clean, text_augmentation])
    pipeline.build()
    print('Input:', pipeline.inputs)
    pipeline.publish()


if __name__ == '__main__':
    # test_publish_pipeline()
    test_get_pipeline()
    # test_run_pipeline()
    # test_publish_pipeline_v2()
