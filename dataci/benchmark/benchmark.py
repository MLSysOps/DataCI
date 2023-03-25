#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 23, 2023

Data-centric Benchmark Object
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from dataci.benchmark.bench_type import verify_bench_type
from dataci.repo import Repo

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
            model_name: str,
            train_kwargs: dict = None,
            repo: 'Repo' = None,
    ):
        self.repo = repo or Repo()
        self.type = type
        self.ml_task = ml_task
        self.train_dataset = train_dataset
        self.test_dataset = test_dataset
        self.model_name = model_name
        self.train_kwargs = train_kwargs or dict()
        self.create_date: 'Optional[datetime]' = datetime.now()
        self.result_dir = None

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

    def run(self, auto_save: bool = True):
        """Run benchmark
        """

        if self.ml_task == 'text_classification':
            from .text_classification import parse_args
            args = [
                       f'--train_dataset={self.train_dataset.dataset_files}',
                       f'--test_dataset={self.test_dataset.dataset_files}',
                       f'--model_name={self.model_name}',
                       f'--id_col={self.train_dataset.id_column}',
                       f'--exp_root={self.repo.benchmark_dir}',
                   ] + [f'--{k}={v}' for k, v in self.train_kwargs.items()]
        else:
            raise ValueError(f'Invalid ml_task {self.ml_task}')
        logger.info(
            f'Run data-centric benchmarking: type={self.type}, ml_task={self.ml_task}\n'
            'args: ' + ' '.join(args))
        args = parse_args(args)
        # TODO: better way to convert none json serializable to str
        self.train_kwargs = {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()}
        # self.result_dir = str(main(args))

        if auto_save:
            # Save benchmark result to DB
            self.save()

    def save(self):
        """Save benchmark result to DB
        """
        from dataci.db.benchmark import create_one_benchmark

        create_one_benchmark(self.dict())
        logger.info(f'Save benchmark to db: {self}')

    def dict(self):
        return {
            'type': self.type,
            'ml_task': self.ml_task,
            'train_dataset_name': self.train_dataset.name,
            'train_dataset_version': self.train_dataset.version,
            'test_dataset_name': self.test_dataset.name,
            'test_dataset_version': self.test_dataset.version,
            'model_name': self.model_name,
            'train_kwargs': json.dumps(self.train_kwargs),
            'result_dir': self.result_dir,
            'timestamp': int(self.create_date.timestamp()),
        }

    @classmethod
    def from_dict(cls, config: dict):
        from dataci.dataset import get_dataset

        config['train_dataset'] = get_dataset(**config['train_dataset'])
        config['test_dataset'] = get_dataset(**config['test_dataset'])
        config['train_kwargs'] = json.loads(config['train_kwargs'])
        self = cls(**config)
        self.create_date = datetime.fromisoformat(config['timestamp'])
