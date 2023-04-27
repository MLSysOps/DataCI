#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import re

NAME_PATTERN = re.compile(r'^[a-z][\w:]*$', flags=re.IGNORECASE)
VERSION_PATTERN = re.compile(r'latest|[a-f\d]{7,40}', flags=re.IGNORECASE)
DATASET_IDENTIFIER_PATTERN = re.compile(
    r'^([a-z][\w:]*)(?:@(latest|[a-z\d]{7,40}))?$', flags=re.IGNORECASE
)
LIST_DATASET_IDENTIFIER_PATTERN = re.compile(
    r'^([\w:.*[\]]+?)(?:@([\da-f]{1,40}))?$', re.IGNORECASE
)


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
