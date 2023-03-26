#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 26, 2023
"""
import argparse

from dataci.benchmark import list_benchmarks
from dataci.command.utils import table_groupby


def ls(args):
    """List all benchmarks
    """
    benchmarks = list_benchmarks(args.train_dataset_name)
    cur_train_dataset, cur_test_dataset = None, None
    for (train_dataset, test_dataset, type_, ml_task, model_name), groups in \
            table_groupby(benchmarks, ['train_dataset.name', 'test_dataset', 'type', 'ml_task', 'model_name']):
        # New train dataset, test dataset
        if cur_train_dataset != train_dataset or cur_test_dataset != test_dataset:
            cur_train_dataset = train_dataset
            cur_test_dataset = test_dataset
            print(f'Train dataset: {train_dataset}, Test dataset: {test_dataset}')
        print(f'Type: {type_}, ML task: {ml_task}, Model name: {model_name}')
        print('Dataset version, Test Acc')
        for benchmark in groups:
            print(f'  {benchmark.train_dataset} {benchmark.metrics["test"]["acc"]}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI dataset')
    subparser = parser.add_subparsers()

    list_parser = subparser.add_parser('ls')
    list_parser.add_argument('train_dataset_name', type=str, nargs='?', default=None, help='Train dataset name')
    list_parser.set_defaults(func=ls)

    args_ = parser.parse_args()
    args_.func(args_)
