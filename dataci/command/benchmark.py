#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 26, 2023
"""
import argparse

from dataci.benchmark import list_benchmarks


def ls(args):
    """List all benchmarks
    """
    benchmarks = list_benchmarks(args.train_dataset_name)
    for benchmark in benchmarks:
        print(benchmark)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI dataset')
    subparser = parser.add_subparsers()

    list_parser = subparser.add_parser('ls')
    list_parser.add_argument('train_dataset_name', type=str, nargs='?', default=None, help='Train dataset name')
    list_parser.set_defaults(func=ls)

    args_ = parser.parse_args()
    args_.func(args_)
