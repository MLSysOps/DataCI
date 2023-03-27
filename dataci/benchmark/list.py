#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 26, 2023
"""
from typing import TYPE_CHECKING

from dataci.db.benchmark import get_many_benchmarks

if TYPE_CHECKING:
    from typing import List
    from dataci.benchmark.benchmark import Benchmark


def list_benchmarks(train_dataset_name: str = None) -> 'List[Benchmark]':
    """List all benchmarks
    """
    from dataci.benchmark.benchmark import Benchmark

    train_dataset_name = train_dataset_name or '*'
    benchmark_dicts = get_many_benchmarks(train_dataset_name)

    benchmarks = list()
    for benchmark_dict in benchmark_dicts:
        benchmark = Benchmark.from_dict(benchmark_dict)
        benchmarks.append(benchmark)

    return benchmarks
