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

    benchmark = Benchmark(
        type='data_augmentation',
        ml_task='text_classification',
        model_name='bert-base-cased',
        train_dataset=train_dataset,
        test_dataset=test_dataset,
        train_kwargs=dict(
            epochs=1,
            batch_size=4,
            learning_rate=1e-5,
            logging_steps=1,
            max_train_steps_per_epoch=10,
            max_val_steps_per_epoch=10,
            seed=42,
        ),
    )
    benchmark.run()


def test_get_benchmark():
    from dataci.benchmark import list_benchmarks

    benchmark = list_benchmarks('train_data_pipeline:text_aug')[0]
    benchmark.get_prediction('train')


if __name__ == '__main__':
    import pandas as pd

    pd.read_csv(
        'D:\\Code\\PycharmProjects\\DataCI\\.dataci\\benchmark\\20230327-214849_task=text_classification_model=bert-base-cased_lr=1.00E-05_b=4_j=4\\pred\\train_preds_epoch=2.csv')
    # test_text_classification_benchmark()
    # test_get_benchmark()
    # pd.read_csv(r'D:\Code\PycharmProjects\DataCI\.dataci\benchmark\20230327-214849_task=text_classification_model=bert-base-cased_lr=1.00E-05_b=4_j=4\pred\train_preds_epoch=2.csv')
