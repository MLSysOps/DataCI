#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 23, 2023

Data-centric Benchmark Object
"""
import logging
from typing import TYPE_CHECKING

from dataci.benchmark.bench_type import verify_bench_type

if TYPE_CHECKING:
    from dataci.dataset.dataset import Dataset

logger = logging.getLogger(__name__)


class Benchmark(object):
    def __init__(
            self,
            type: str,
            ml_task: str,
            train_dataset: 'Dataset',
            test_dataset: 'Dataset',
            model: str,
            train_kwargs: dict = None,
    ):
        self.type = type
        self.ml_task = ml_task
        self.train_dataset = train_dataset
        self.test_dataset = test_dataset
        self.model = model
        self.train_kwargs = train_kwargs or dict()

        self._verify()

    def _verify(self):
        """Verify if the provided datasets are valid for the benchmark
        """
        if not verify_bench_type(self.type, self.train_dataset, self.test_dataset):
            raise ValueError(
                f'Invalid benchmark type {self.type} for datasets {self.train_dataset} and {self.test_dataset}'
            )
        if self.ml_task not in ['text_classification']:
            raise ValueError(f'Invalid ml_task {self.ml_task}')

    def run(self):
        """Run benchmark
        """

        if self.ml_task == 'text_classification':
            from .text_classification import main, parse_args
            args = [
                       f'--train_dataset={self.train_dataset.dataset_files}',
                       f'--test_dataset={self.test_dataset.dataset_files}',
                       f'--model={self.model}', f'--id_col={self.train_dataset.id_column}'
                   ] + [f'--{k}={v}' for k, v in self.train_kwargs.items()]
        else:
            raise ValueError(f'Invalid ml_task {self.ml_task}')
        logger.info(
            f'Run data-centric benchmarking: type={self.type}, ml_task={self.ml_task}\n'
            'args: ' + ' '.join(args))
        main(parse_args(args))
