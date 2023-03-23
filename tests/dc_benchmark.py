#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 23, 2023
"""
from dataci.dataset import get_dataset


def test_text_classification_benchmark():
    from dataci.benchmark.benchmark import Benchmark

    train_dataset = get_dataset('train_data_pipeline:text_aug')
    test_dataset = get_dataset('text_raw_val')

    Benchmark(
        type='data_augmentation',
        ml_task='text_classification',
        model='bert-base-cased',
        train_dataset=train_dataset,
        test_dataset=test_dataset,
    )


if __name__ == '__main__':
    test_text_classification_benchmark()
