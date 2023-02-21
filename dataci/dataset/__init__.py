#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import logging
import os
import subprocess
import time
from pathlib import Path

import yaml

from dataci.dataset.utils import generate_dataset_version_id
from dataci.repo import Repo

logger = logging.getLogger(__name__)


def publish_dataset(repo: Repo, dataset_name, targets, output_pipeline=None, log_message=None, parent_dataset=None):
    targets = Path(targets).resolve()
    output_pipeline = output_pipeline or list()
    log_message = log_message or ''
    parent_dataset = parent_dataset or None

    # check dataset splits
    splits = list()
    for split_dir in os.scandir(targets):
        if split_dir.is_dir():
            splits.append(split_dir.name)
    for split in splits:
        if split not in ['train', 'val', 'test']:
            raise ValueError(f'{split} is not a valid split name. Expected "train", "val", "test".')
    dataset_files = [(targets / split).resolve() for split in splits]

    # Data file version controlled by DVC
    logger.info(f'Caching dataset files: {dataset_files}')
    subprocess.run(['dvc', 'add'] + list(map(str, dataset_files)))

    # Save tracked dataset to repo
    repo_dataset_path = repo.dataset_dir / dataset_name
    repo_dataset_path.mkdir(exist_ok=True)
    # Patch meta data to each generated .dvc file
    meta = {
        'version': generate_dataset_version_id(targets),
        'timestamp': int(time.time()),
        'output_pipeline': output_pipeline,
        'log_message': log_message,
        'parent_dataset': parent_dataset,
    }
    for file in dataset_files:
        dvc_filename = file.with_suffix('.dvc')
        with open(dvc_filename, 'r') as f:
            dvc_config = yaml.safe_load(f)
        dvc_config['meta'] = meta
        # Save tracked dataset to repo
        dataset_tracked_file = repo_dataset_path / (dvc_filename.stem + ".yaml")
        print(dataset_tracked_file)
        logging.info(f'Adding meta data: {dataset_tracked_file}')
        with open(dataset_tracked_file, 'a') as f:
            yaml.safe_dump({meta['version']: dvc_config}, f)
