#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import hashlib
import re
from pathlib import Path

NAME_PATTERN = re.compile(r'^[a-z]\w*$', flags=re.IGNORECASE)
VERSION_PATTERN = re.compile(r'latest|[a-f\d]{7,40}', flags=re.IGNORECASE)
DATASET_IDENTIFIER_PATTERN = re.compile(
    r'^([a-z]\w*)(?:@(latest|[a-z\d]{7,40}))?(?:\[(train|val|test|all)])?$', flags=re.IGNORECASE
)


def generate_dataset_identifier(dataset_name: str, version: str = None, split = None):
    """Generate dataset identifier string

    Args:
        dataset_name (str): The dataset name should be a valid: start with alphabet and contains any
            number, alphabet, and underscore ("_"). Example:
                `1a` (invalid)
                `_` (invalid)
                `_abc` (invalid)
                `abc123` (valid)
        version (str): The version ID contains 7-40 hex characters (0-9, a-f).
        split (str): The split of dataset. One of "train", "val", "test", and "all".
    """
    dataset_name = str(dataset_name)
    version = str(version).lower() or 'latest'
    split = str(split).lower() or 'all'
    if not NAME_PATTERN.match(dataset_name):
        raise ValueError(f'Invalid dataset_name="{dataset_name}".')
    if not VERSION_PATTERN.match(version):
        raise ValueError(
            f'Invalid version="{version}": expected a hex string and with a length of 7 to 40.'
        )
    if split not in ['train', 'val', 'test', 'all']:
        raise ValueError(f'Invalid split="{split}": expected one of ["train", "val", "test", "all"].')

    return f'{dataset_name}@{version}[{split}]'


def parse_dataset_identifier(name_str: str):
    """Parse dataset identifier to dataset name, version, and split.

    Raises:
        ValueError: Raised if invalid dataset identifier is passed. The valid dataset identifier
            should contain: dataset name, optional version with 7-40 hex string or latest string,
            and optional split tag in one of [train, val, test, all].

    Examples:
        >>> parse_dataset_identifier('a1')
        {'dataset_name': 'a1', 'version': 'latest', 'split': 'all'}
        >>> parse_dataset_identifier('a1@123456f')
        {'dataset_name': 'a1', 'version': '123456f', 'split': 'all'}
        >>> parse_dataset_identifier('a1@123456f[train]')
        {'dataset_name': 'a1', 'version': '123456f', 'split': 'train'}
        >>> parse_dataset_identifier('a1@123')
        ValueError: Invalid dataset identifier: a1@123
    """
    match = DATASET_IDENTIFIER_PATTERN.match(name_str)
    if not match:
        raise ValueError(f'Invalid dataset identifier: {name_str}')
    dataset_name, version, split = match.groups()
    version = (version and version.lower()) or 'latest'
    split = (split and split.lower()) or 'all'

    return {'dataset_name': dataset_name, 'version': version, 'split': split}


def generate_dataset_version_id(dataset_path, yield_pipeline=None, log_message=None, parent_dataset=None):
    # TODO: Change the version ID generating logic: https://www.quora.com/How-are-Git-commit-IDs-generated
    # Find .dvc traced data files
    if not isinstance(dataset_path, list):
        dataset_path = [dataset_path]
    dataset_path = [Path(d) for d in dataset_path]
    yield_pipeline = yield_pipeline or list()
    log_message = log_message or ''
    parent_dataset = parent_dataset or ''

    dataset_trackers = list(map(lambda x: str(x) + '.dvc', dataset_path))
    dataset_trackers.sort()

    if len(dataset_trackers) != len(dataset_path):
        raise ValueError(
            f'At least one dataset trackers (*.dvc) not found for directories {dataset_path}. '
        )
    dataset_obj = b''
    for dataset_tracker in dataset_trackers:
        with open(dataset_tracker, 'rb') as f:
            dataset_obj += f.read()
    output_pipeline_id_obj = ','.join(yield_pipeline).encode()
    log_message_obj = log_message.encode()
    parent_dataset = parent_dataset.encode()

    packed_obj = dataset_obj + output_pipeline_id_obj + log_message_obj + parent_dataset
    return hashlib.sha1(packed_obj).hexdigest()
