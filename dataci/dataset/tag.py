#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 25, 2023
"""
from typing import TYPE_CHECKING

from dataci.db.dataset import create_dataset_tag

if TYPE_CHECKING:
    from .dataset import Dataset


def tag(dataset: 'Dataset', tag_name: str, tag_version: str):
    """
    Tag dataset with a version.
    """
    if not dataset.__published:
        raise RuntimeError('Not able to tag a Dataset that is not published.')
    create_dataset_tag(dataset.name, dataset.version, tag_name, tag_version)
