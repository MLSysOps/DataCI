#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import fnmatch
import logging
import re
import subprocess
from collections import OrderedDict, defaultdict

import yaml

from dataci.db import db_connection
from dataci.repo import Repo
from .dataset import Dataset
from .utils import generate_dataset_version_id, parse_dataset_identifier, generate_dataset_identifier

logger = logging.getLogger(__name__)

LIST_DATASET_IDENTIFIER_PATTERN = re.compile(
    r'^([\w.*[\]]+?)(?:@([\da-f]{1,40}))?(?:\[(train|val|test|all)])?$', re.IGNORECASE
)


def publish_dataset(repo: Repo, dataset_name, targets, yield_pipeline=None, parent_dataset=None, log_message=None):
    if not isinstance(targets, list):
        targets = [targets]

    # Data file version controlled by DVC
    logger.info(f'Caching dataset files: {targets}')
    subprocess.run(['dvc', 'add'] + list(map(str, targets)))

    for dataset_file in targets:
        dataset = Dataset(
            name=dataset_name, dataset_files=dataset_file, yield_pipeline=yield_pipeline,
            parent_dataset=parent_dataset, log_message=log_message, repo=repo,
        )
        dao = dataset.dict()
        # Save dataset with version to repo
        with db_connection:
            db_connection.execute(
                """
                INSERT INTO dataset (name, version, yield_pipeline, log_message, timestamp, file_config, 
                parent_dataset_name, parent_dataset_version)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    dao['name'], dao['version'], dao['yield_pipeline'], dao['log_message'], dao['timestamp'],
                    dao['file_config'], dao['parent_dataset_name'], dao['parent_dataset_version'],
                )
            )
        logging.info(f'Adding dataset to db: {dataset}')


def list_dataset(repo: Repo, dataset_identifier=None, tree_view=True):
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
        tree_view (bool): View the queried dataset as a 3-level-tree, level 1 is dataset name, level 2 is split tag,
            and level 3 is version.

    Returns:
        A dict (tree_view=True, default) or a list (tree_view=False) of dataset information.
            If view as a tree, the format is {dataset_name: {split_tag: {version_id: dataset_info}}}.

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
    dataset_identifier = dataset_identifier or '*'
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
    ret_dataset_list = list()
    for dataset in datasets:
        # Check matched splits
        splits = list((repo.dataset_dir / dataset).glob(f'{split}.yaml'))

        # Check matched version
        for split_path in splits:
            split = split_path.stem
            with open(split_path) as f:
                dataset_version_config: dict = yaml.safe_load(f)
            versions = fnmatch.filter(dataset_version_config.keys(), version)
            for ver in versions:
                dataset_obj = Dataset(
                    name=dataset, version=ver, config=dataset_version_config[ver],
                    repo=repo,
                )
                if tree_view:
                    ret_dataset_dict[dataset][split][ver] = dataset_obj
                else:
                    ret_dataset_list.append(dataset_obj)

    return ret_dataset_dict if tree_view else ret_dataset_list
