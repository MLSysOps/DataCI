#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 26, 2023
"""
import argparse
import fnmatch

from dataci.benchmark import list_benchmarks
from dataci.command.utils import table_groupby


def ls(args):
    """List all benchmarks
    """
    # Get benchmarks by query
    benchmarks = list_benchmarks(args.train_dataset_name)

    # Parse metrics
    metrics_names = list()
    for metric in args.metrics.split(',') if args.metrics else list():
        metrics_name = metric.split('/')
        assert len(metrics_name) == 2, \
            f'Invalid metric name: {metric}. You should specify the stage name and metric name, e.g., test/acc.'
        metrics_names.append(metrics_name)

    # Display benchmarks
    cur_train_dataset, cur_test_dataset = None, None
    for (train_dataset, test_dataset, type_, ml_task, model_name), groups in \
            table_groupby(benchmarks, ['train_dataset.name', 'test_dataset', 'type', 'ml_task', 'model_name']):
        # New train dataset, test dataset
        if cur_train_dataset != train_dataset or cur_test_dataset != test_dataset:
            cur_train_dataset = train_dataset
            cur_test_dataset = test_dataset
            print(
                f'Train dataset: {train_dataset}, Test dataset: {test_dataset}')
        print(f'Type: {type_}, ML task: {ml_task}, Model name: {model_name}')
        # Dataset version, metrics name1, metrics name2, ...
        print(' '.join(
            [f'{"Dataset version":15}'] +
            [f'{(stage + " " + metrics).capitalize():10}' for stage, metrics in metrics_names])
        )

        for benchmark in groups:
            print(' '.join(
                [f'{benchmark.train_dataset.version[:7]:15}'] +
                [f'{benchmark.metrics[stage][metrics]:10f}' for stage,
                    metrics in metrics_names]
            ))


def ls_pipeline(args):
    pipeline_name = (args.pipeline_name or '') + '*'

    # Get all benchmarks
    benchmarks = list_benchmarks()
    # Filter benchmarks by pipeline
    # TODO: use database query to filter
    for benchmark in benchmarks:
        if not fnmatch.fnmatch(benchmark.train_pipeline.name, pipeline_name):
            benchmarks.remove(benchmark)

    # Parse metrics
    metrics_names = list()
    for metric in args.metrics.split(',') if args.metrics else list():
        metrics_name = metric.split('/')
        assert len(metrics_name) == 2, \
            f'Invalid metric name: {metric}. You should specify the stage name and metric name, e.g., test/acc.'
        metrics_names.append(metrics_name)

    # Display benchmarks
    for (type_, ml_task, model_name), groups in \
            table_groupby(benchmarks, ['type', 'ml_task', 'model_name']):
        # benchmark type (type, ml_task, model_name)
        print(f'Type: {type_}, ML task: {ml_task}, Model name: {model_name}')
        pipeline_dict = dict()
        for train_pipeline, pipeline_groups in \
            table_groupby(list(groups), ['train_pipeline']):
            pipeline_dict[repr(train_pipeline)] = {
                str(benchmark.train_dataset.parent_dataset): benchmark for benchmark in pipeline_groups
            }
        
        dataset_set = set()
        for dataset_dict in pipeline_dict.values():
            dataset_set.update(dataset_dict.keys())
        
        #           Dataset
        # <null>    dataset1    dataset2    dataset3    ...
        for pipeline, benchmark_dict in pipeline_dict.items():
            print(f'Pipeline: {pipeline}')
            # Dataset version, metrics name1, metrics name2, ...
            print(' '.join(
                [f'{"Dataset version":15}'] +
                [f'{(stage + " " + metrics).capitalize():10}' for stage, metrics in metrics_names])
            )
            for train_dataset, benchmark in benchmark_dict.items():
                print(' '.join(
                    [f'{train_dataset:15}'] +
                    [f'{benchmark.metrics[stage][metrics]:10f}' for stage,
                        metrics in metrics_names]
                ))


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI dataset')
    subparser = parser.add_subparsers()

    list_parser = subparser.add_parser('ls')
    list_parser.add_argument('-me', '--metrics', type=str,
                             help='Metrics to show. Use comma to separate multiple metrics. (e.g., test/acc,train/loss)')
    list_parser.add_argument('train_dataset_name', type=str,
                             nargs='?', default=None, help='Train dataset name')
    list_parser.set_defaults(func=ls)

    list_pipeline_parser = subparser.add_parser('lsp')
    list_pipeline_parser.add_argument('-me', '--metrics', type=str,
                                      help='Metrics to show. Use comma to separate multiple metrics. (e.g., test/acc,train/loss)')
    list_pipeline_parser.add_argument('pipeline_name', type=str, nargs='?', default=None,
                                      help='Pipeline name')
    list_pipeline_parser.set_defaults(func=ls_pipeline)

    args_ = parser.parse_args()
    args_.func(args_)
