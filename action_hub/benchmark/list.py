#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 26, 2023
"""
from typing import TYPE_CHECKING

from dataci.benchmark.benchmark import Benchmark

from dataci.dataset.utils import LIST_DATASET_IDENTIFIER_PATTERN
from dataci.db.benchmark import get_many_benchmarks

if TYPE_CHECKING:
    from typing import List


def list_benchmarks(train_dataset_identifier: str = None) -> 'List[Benchmark]':
    """List all benchmarks
    """
    train_dataset_identifier = train_dataset_identifier or '*'
    matched = LIST_DATASET_IDENTIFIER_PATTERN.match(train_dataset_identifier)
    if not matched:
        raise ValueError(f'Invalid train dataset identifier {train_dataset_identifier}')
    name, version = matched.groups()
    name = name or '*'
    version = (version or '').lower() + '*'

    benchmark_dicts = get_many_benchmarks(name, version)

    benchmarks = list()
    for benchmark_dict in benchmark_dicts:
        benchmark = Benchmark.from_dict(benchmark_dict)
        benchmarks.append(benchmark)

    return benchmarks
