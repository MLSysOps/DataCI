#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 15, 2023
"""
import logging
import subprocess
from typing import TYPE_CHECKING

from dataci.db.dataset import create_one_dataset


if TYPE_CHECKING:
    from .dataset import Dataset


logger = logging.getLogger(__name__)


def publish(dataset: 'Dataset' = ...):
    #####################################################################
    # Step 1: Save dataset files by DVC
    #####################################################################
    logger.info(f'Caching dataset files: {dataset.dataset_files}')
    subprocess.run(['dvc', 'add'] + [str(dataset.dataset_files)])

    #####################################################################
    # Step 2: Publish dataset to DB
    #####################################################################
    create_one_dataset(dataset.dict())
    logger.info(f'Adding dataset to db: {dataset}')
    
    