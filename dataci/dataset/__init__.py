#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import fnmatch
import logging
import os
import re
import subprocess
import time
from collections import OrderedDict, defaultdict
from copy import deepcopy
from functools import lru_cache
from pathlib import Path

import yaml

from dataci.dataset.utils import generate_dataset_version_id, parse_dataset_identifier
from dataci.repo import Repo

logger = logging.getLogger(__name__)

LIST_DATASET_IDENTIFIER_PATTERN = re.compile(
    r'^([\w.*[\]]+?)(?:@([\da-f]{1,40}))?(?:\[(train|val|test|all)])?$', re.IGNORECASE
)


def publish_dataset(repo: Repo, dataset_name, targets, yield_pipeline=None, parent_dataset=None, log_message=None, ):
    targets = Path(targets).resolve()
    yield_pipeline = yield_pipeline or list()
    parent_dataset = parent_dataset or None
    log_message = log_message or ''

    # check dataset splits
    splits = list()
    for split_dir in os.scandir(targets):
        if split_dir.is_dir():
            splits.append(split_dir.name)
    for split in splits:
        if split not in ['train', 'val', 'test']:
            raise ValueError(f'{split} is not a valid split name. Expected "train", "val", "test".')
    dataset_files = {split: (targets / split).resolve() for split in splits}

    # Data file version controlled by DVC
    logger.info(f'Caching dataset files: {dataset_files.values()}')
    subprocess.run(['dvc', 'add'] + list(map(str, dataset_files.values())))

    repo_dataset_path = repo.dataset_dir / dataset_name

    for split, dataset_file in dataset_files.items():
        dataset = Dataset(
            dataset_name=dataset_name, split=split, dataset_files=dataset_file, yield_pipeline=yield_pipeline,
            parent_dataset=parent_dataset, log_message=log_message,
        )
        # Save tracked dataset with version to repo
        dataset_config_file = repo_dataset_path / (split + ".yaml")
        logging.info(f'Adding meta data: {dataset_config_file}')
        with open(dataset_config_file, 'a') as f:
            yaml.safe_dump({dataset.version: dataset.config}, f, sort_keys=False)


def list_dataset(repo: Repo, dataset_identifier=None):
    """
    List dataset with optional dataset identifier to query.

    Args:
        repo:
        dataset_identifier: Dataset name with optional version and optional split information to query.
            In this field, it supports three components in the format of dataset_name@version[split].
            - dataset name: Support glob. Default to query for all datasets.
            - version (optional): Version ID or the starting few characters of version ID. It will search
                all matched versions of this dataset. Default to list all versions.
            - split (optional): In one of "train", "val", "split" or "all". Default to list all splits.

    Returns:
        A dict of dataset information. The format is {dataset_name: {split_tag: {version_id: dataset_info}}}.

    Examples:
        >>> repo = Repo()
        >>> list_dataset(repo=repo)
        {'dataset1': {'train': {'1234567a': ..., '1234567b': ...}, 'test': ...}, 'dataset12': ...}
        >>> list_dataset(repo=repo, dataset_identifier='dataset1')
        {'dataset1': {'train': {'1234567a': ..., '1234567b': ...}, 'test': ...}}
        >>> list_dataset(repo=repo, dataset_identifier='data*')
        {'dataset1': {'train': {'1234567a': ..., '1234567b': ...}, 'test': ...}, 'dataset12': ...}
        >>> list_dataset(repo=repo, dataset_identifier='dataset1@1')
        {'dataset1': {'train': {'1234567a': ..., '1234567b': ...}, 'test': ...}}
        >>> list_dataset(repo=repo, dataset_identifier='dataset1@1234567a')
        {'dataset1': {'train': {'1234567a': ...}, 'test': ...}}
        >>> list_dataset(repo=repo, dataset_identifier='dataset1[test]')
        {'dataset1': {'test': ...}}
        >>> list_dataset(repo=repo, dataset_identifier='dataset1[*]')
        ValueError: Invalid dataset identifier dataset1[*]
    """
    matched = LIST_DATASET_IDENTIFIER_PATTERN.match(dataset_identifier)
    if not matched:
        raise ValueError(f'Invalid dataset identifier {dataset_identifier}')
    dataset_name, version, split = matched.groups()
    dataset_name = dataset_name or '*'
    version = (version or '').lower() + '*'
    split = (split or '*').lower()
    if split == 'all':
        split = '*'

    # Check matched datasets
    datasets = list()
    for folder in repo.dataset_dir.glob(dataset_name):
        if folder.is_dir():
            datasets.append(folder.name)

    ret_dataset_dict = defaultdict(lambda: defaultdict(OrderedDict))
    for dataset in datasets:
        # Check matched splits
        splits = list((repo.dataset_dir / dataset).glob(f'{split}.yaml'))

        # Check matched version
        for split in splits:
            with open(split) as f:
                dataset_version_config: dict = yaml.safe_load(f)
            versions = fnmatch.filter(dataset_version_config.keys(), version)
            for ver in versions:
                ret_dataset_dict[dataset][split.stem][ver] = dataset_version_config[ver]['meta']
    return ret_dataset_dict


class Dataset(object):
    def __init__(
            self,
            dataset_name,
            version=None,
            split=None,
            dataset_files=None,
            yield_pipeline=None,
            parent_dataset=None,
            log_message=None,
            meta=None,
    ):
        self.dataset_name = dataset_name
        # Filled if the dataset is published
        self.version = version
        self.split = split
        self.meta = meta
        # Filled if the dataset is not published
        self.dataset_files = Path(dataset_files) if dataset_files else None
        self.create_date = None
        self.yield_pipeline = yield_pipeline
        self.parent_dataset = parent_dataset
        self.log_message = log_message
        self.shadow = False

        if self.meta:
            self._parse_meta_info()
        else:
            assert self.version is None, 'The dataset is creating with an assigned version'
            self._build_meta_info()

    def _parse_meta_info(self):
        self.create_date = self.meta['timestamp']
        self.yield_pipeline = self.meta['yield_pipeline']
        self.parent_dataset = self.meta['parent_dataset']
        self.log_message = self.meta['log_message']
        self.shadow_version = self.meta['version'] if self.meta != self.version else None

    def _build_meta_info(self):
        self.create_date = int(time.time())
        self.yield_pipeline = self.yield_pipeline or list()
        self.log_message = self.log_message or ''

        self.meta = {
            'timestamp': self.create_date,
            'parent_dataset': self.parent_dataset,
            'yield_pipeline': self.yield_pipeline,
            'log_message': self.log_message,
            'version': generate_dataset_version_id(
                self.dataset_files.parent, self.yield_pipeline, self.log_message, self.parent_dataset
            )
        }
        self.version = self.meta['version']

    @property
    @lru_cache(maxsize=None)
    def dvc_config(self):
        if self.dataset_files:
            dvc_filename = self.dataset_files.with_suffix('.dvc')
            with open(dvc_filename, 'r') as f:
                dvc_config = yaml.safe_load(f)
            return dvc_config

    @property
    @lru_cache(maxsize=None)
    def config(self):
        config = deepcopy(self.dvc_config)
        config['meta'] = self.meta
        return config
