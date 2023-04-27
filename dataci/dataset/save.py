#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 15, 2023
"""
import logging
from shutil import copy2
from typing import TYPE_CHECKING

from dataci.connector.s3 import download as s3_download
from dataci.db.dataset import create_one_dataset, get_next_version_id

if TYPE_CHECKING:
    from .dataset import Dataset

logger = logging.getLogger(__name__)


def save(dataset: 'Dataset' = ...):
    version = get_next_version_id(dataset.name)
    dataset.version = version
    #####################################################################
    # Step 1: Save dataset to mount cloud object storage
    #####################################################################
    logger.info(f'Save dataset files to mounted S3: {dataset.workspace.data_dir}')
    dataset_cache_dir = dataset.workspace.data_dir / dataset.name / str(version)
    dataset_cache_dir.mkdir(parents=True, exist_ok=True)
    # Dataset files is a S3 path
    dataset_files = str(dataset.dataset_files)
    if dataset_files.startswith('s3://'):
        # Copy S3 files to cloud object storage
        s3_download(dataset_files, dataset_cache_dir)
    else:
        # Copy local files to cloud object storage
        # FIXME: only support a single file now
        copy2(dataset.dataset_files, dataset_cache_dir)

    #####################################################################
    # Step 2: Publish dataset to DB
    #####################################################################
    create_one_dataset(dataset.dict())
    logger.info(f'Adding dataset to db: {dataset}')
