#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 26, 2023
"""
from pathlib import Path

import torch

from dataci.benchmark import Benchmark
from dataci.dataset import list_dataset

FILE_WORK_DIR = Path(__file__).parent.resolve()

if __name__ == '__main__':
    print('=' * 80)
    print('Step 1: Benchmark text classification dataset v1')
    print('=' * 80)

    # Get all versions of the text classification dataset
    print('Get all versions of the text classification dataset')
    text_classification_datasets = list_dataset('train_data_pipeline:text_aug', tree_view=False)
    print(text_classification_datasets)
    # Sort by created date
    print('Sort by created date')
    text_classification_datasets.sort(key=lambda x: x.create_date)
    for dataset in text_classification_datasets:
        print(dataset, dataset.create_date)
    train_dataset = text_classification_datasets[0]
    print('Train dataset:', train_dataset)

    # Get all versions of the raw text dataset val split
    print('Get all versions of the raw text dataset val split')
    text_raw_val_datasets = list_dataset('text_raw_val', tree_view=False)
    # Sort by created date
    print('Sort by created date')
    text_raw_val_datasets.sort(key=lambda x: x.create_date)
    test_dataset = text_raw_val_datasets[0]
    print('Test dataset:', test_dataset)

    # Run the benchmark
    print('Checking CUDA availability...')
    if torch.cuda.is_available():
        print(f'Found CUDA: {torch.device("cuda")}')
        other_train_kwargs = dict(batch_size=32)
    else:
        print(
            'Not found CUDA. I will adjust the benchmarking parameters for you:\n'
            '- Less batch size\n'
            '- Train a few steps instead of the whole iteration\n'
            '- Evaluate and test with few steps instead of the whole iteration\n'
            '- Smaller log printing frequency'
        )
        other_train_kwargs = dict(batch_size=4, logging_steps=1, max_train_steps_per_epoch=10,
                                  max_val_steps_per_epoch=10)

    train_kwargs = {
        'epochs': 3,
        'learning_rate': 1e-5,
        'seed': 42,
        **other_train_kwargs,
    }

    print('Benchmarking...')
    benchmark = Benchmark(
        type='data_augmentation',
        ml_task='text_classification',
        model_name='bert-base-cased',
        train_dataset=train_dataset,
        test_dataset=test_dataset,
        train_kwargs=train_kwargs,
    )
    benchmark.run()
    print('Finish benchmarking!')

    # Print the benchmark result
    print('Benchmark result saved at:')
    print(benchmark.result_dir)
    print('Benchmark metrics:')
    print(benchmark.metrics)

    # enchmark all text classification datasets (v2 - v4)
    print('=' * 80)
    print('Step 2: Benchmark all text classification datasets (v2 - v4)')
    print('=' * 80)
    for text_classification_dataset in text_classification_datasets[1:]:
        print(f'Benchmarking {text_classification_dataset}')
        benchmark = Benchmark(
            type='data_augmentation',
            ml_task='text_classification',
            model_name='bert-base-cased',
            train_dataset=text_classification_dataset,
            test_dataset=test_dataset,
            train_kwargs=train_kwargs,
        )
        # Run benchmark
        benchmark.run()
