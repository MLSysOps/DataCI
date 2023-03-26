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
    import os
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
            result_dir: 'Optional[os.PathLike]' = None,
            repo: 'Repo' = None,
            **kwargs,
    ):
        self.repo = repo or Repo()
        self.type = type
        self.ml_task = ml_task
        self.train_dataset = train_dataset
        self.test_dataset = test_dataset
        self.model_name = model_name
        self.train_kwargs = train_kwargs or dict()
        self.create_date: 'Optional[datetime]' = datetime.now()
        self.result_dir = Path(result_dir) if result_dir else None

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
            from .text_classification import parse_args, main
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
        self.train_kwargs = vars(args)
        result_dir = main(args)
        self.result_dir = Path(result_dir)

        if auto_save:
            # Save benchmark result to DB
            self.save()

    def save(self):
        """Save benchmark result to DB
        """
        from dataci.db.benchmark import create_one_benchmark

        create_one_benchmark(self.dict())
        logger.info(f'Save benchmark to db: {self}')

    @property
    def metrics(self):
        """Get benchmark metrics
        """
        if not self.result_dir:
            raise ValueError('Benchmark not run yet')

        metrics = {'train': dict(), 'val': dict(), 'test': None}
        # Load metrics from metrics dir
        # 1. Load test metrics
        with open(self.result_dir / 'metrics' / 'test_metrics.json') as f:
            metrics['test'] = json.load(f)
        # 2. Load train metrics
        for train_metrics_path in (self.result_dir / 'metrics').glob('train_metrics_epoch=[0-9]*.json'):
            epoch = int(train_metrics_path.stem.split('=')[-1])
            with open(train_metrics_path) as f:
                metrics['train'][epoch] = json.load(f)
        # 3. Load val metrics
        for val_metrics_path in (self.result_dir / 'metrics').glob('val_metrics_epoch=[0-9]*.json'):
            epoch = int(val_metrics_path.stem.split('=')[-1])
            with open(val_metrics_path) as f:
                metrics['val'][epoch] = json.load(f)

        return metrics

    def dict(self):
        return {
            'type': self.type,
            'ml_task': self.ml_task,
            'train_dataset_name': self.train_dataset.name,
            'train_dataset_version': self.train_dataset.version,
            'test_dataset_name': self.test_dataset.name,
            'test_dataset_version': self.test_dataset.version,
            'model_name': self.model_name,
            'train_kwargs': json.dumps({
                k: str(v) if isinstance(v, Path) else v for k, v in self.train_kwargs.items()}),
            'result_dir': str(self.result_dir),
            'timestamp': int(self.create_date.timestamp()),
        }

    @classmethod
    def from_dict(cls, config: dict):
        from dataci.dataset import get_dataset

        config['train_dataset'] = get_dataset(**config['train_dataset'])
        config['test_dataset'] = get_dataset(**config['test_dataset'])
        config['train_kwargs'] = json.loads(config['train_kwargs'])
        self = cls(**config)
        self.create_date = datetime.fromtimestamp(config['timestamp'])
        return self
