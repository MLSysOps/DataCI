#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING

from dataci.db.dataset import get_one_dataset, get_many_datasets
from .dataset import Dataset
from .utils import DATASET_IDENTIFIER_PATTERN

if TYPE_CHECKING:
    from dataci.repo import Repo
    from typing import Optional

logger = logging.getLogger(__name__)

LIST_DATASET_IDENTIFIER_PATTERN = re.compile(
    r'^([\w.*[\]]+?)(?:@([\da-f]{1,40}))?$', re.IGNORECASE
)


def get_dataset(name, version=None, repo: 'Optional[Repo]' = None):
    # If version is provided along with name
    matched = DATASET_IDENTIFIER_PATTERN.match(str(name))
    if not matched:
        raise ValueError(f'Invalid dataset identifier {name}')
    # Parse name and version
    name, version_ = matched.groups()
    # Only one version is allowed to be provided, either in name or in version
    if version and version_:
        raise ValueError('Only one version is allowed to be provided by name or version.')

    version = version or version_
    version = str(version).lower() if version else 'latest'

    # Check version
    if version != 'latest':
        # Version hash ID should provide 7 - 40 digits
        assert 40 >= len(version) >= 7, \
            'You should provided the length of version ID within 7 - 40 (both included).'
    dataset_dict = get_one_dataset(name=name, version=version)
    dataset_dict['repo'] = repo
    return Dataset.from_dict(dataset_dict)


def list_dataset(dataset_identifier=None, tree_view=True, repo: 'Repo' = None):
    """
    List dataset with optional dataset identifier to query.

    Args:
        dataset_identifier: Dataset name with optional version and optional split information to query.
            In this field, it supports three components in the format of dataset_name@version[split].
            - dataset name: Support glob. Default to query for all datasets.
            - version (optional): Version ID or the starting few characters of version ID. It will search
                all matched versions of this dataset. Default to list all versions.
        tree_view (bool): View the queried dataset as a 3-level-tree, level 1 is dataset name, level 2 is split tag,
            and level 3 is version.

    Returns:
        A dict (tree_view=True, default) or a list (tree_view=False) of dataset information.
            If view as a tree, the format is {dataset_name: {split_tag: {version_id: dataset_info}}}.

    Examples:
        >>> repo = Repo()
        >>> list_dataset(repo=repo)
        {'dataset1': {'1234567a': ..., '1234567b': ...}, 'dataset12': ...}
        >>> list_dataset(repo=repo, dataset_identifier='dataset1')
        {'dataset1': {'1234567a': ..., '1234567b': ...}}
        >>> list_dataset(repo=repo, dataset_identifier='data*')
        {'dataset1': {'1234567a': ..., '1234567b': ...}, 'dataset12': ...}
        >>> list_dataset(repo=repo, dataset_identifier='dataset1@1')
        {'dataset1': {'1234567a': ..., '1234567b': ...}}
        >>> list_dataset(repo=repo, dataset_identifier='dataset1@1234567a')
        {'dataset1': {'1234567a': ...}}
    """
    repo = repo or Repo()
    dataset_identifier = dataset_identifier or '*'
    matched = LIST_DATASET_IDENTIFIER_PATTERN.match(dataset_identifier)
    if not matched:
        raise ValueError(f'Invalid dataset identifier {dataset_identifier}')
    name, version = matched.groups()
    name = name or '*'
    version = (version or '').lower() + '*'

    dataset_dict_list = get_many_datasets(name=name, version=version)
    dataset_list = list()
    for dataset_dict in dataset_dict_list:
        dataset_dict['repo'] = repo
        dataset_list.append(Dataset.from_dict(dataset_dict))
    if tree_view:
        dataset_dict = defaultdict(dict)
        for dataset in dataset_list:
            dataset_dict[dataset.name][dataset.version] = dataset
        return dataset_dict

    return dataset_list
