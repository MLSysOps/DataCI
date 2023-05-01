#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 21, 2023
"""
import configparser
import logging
import os
import shutil

from dataci.config import CACHE_ROOT, DEFAULT_WORKSPACE, CONFIG_FILE
from dataci.connector.s3 import connect as connect_s3, create_bucket, mount_bucket, unmount_bucket

logger = logging.getLogger(__name__)


class Workspace(object):
    def __init__(self, name: str = None):
        if name is None:
            name = DEFAULT_WORKSPACE
        if name is None:
            raise ValueError('Workspace name is not provided and default workspace is not set.')
        self.name = str(name)
        # Workspace root
        self.root_dir = CACHE_ROOT / self.name

    @property
    def workflow_dir(self):
        return self.root_dir / 'workflow'

    @property
    def data_dir(self):
        return self.root_dir / 'data'

    @property
    def tmp_dir(self):
        return self.root_dir / 'tmp'

    def remove(self):
        # Delete the S3 bucket content
        shutil.rmtree(self.data_dir, ignore_errors=True)
        logger.info(f'Remove all files in bucket {self.data_dir}.')

        # Unmount the S3 bucket
        unmount_bucket(self.data_dir, unmount_ok=True)
        logger.info(f'Unmount bucket {self.data_dir}.')

        # Clear the database
        # FIXME: only remove the tables belong to the current workspace

        # Delete the workspace directory
        shutil.rmtree(self.root_dir)
        logger.info(f'Remove workspace {self.name} from {self.root_dir}.')

        # Delete the workspace from config file
        config = configparser.ConfigParser()
        config.read(CACHE_ROOT / 'config.ini')
        config.remove_option('DEFAULT', 'workspace')
        with open(CACHE_ROOT / 'config.ini', 'w') as f:
            config.write(f)
        logger.info(f'Remove workspace {self.name} from config {CACHE_ROOT / "config.ini"}.')


def create_or_use_workspace(workspace: Workspace):
    # if not workspace.root_dir.exists():
    os.makedirs(workspace.root_dir, exist_ok=True)
    os.makedirs(workspace.workflow_dir, exist_ok=True)
    os.makedirs(workspace.data_dir, exist_ok=True)
    os.makedirs(workspace.tmp_dir, exist_ok=True)
    # Create a S3 bucket for data
    fs = connect_s3()
    # Add a prefix to bucket name to avoid name conflict
    # TODO: change the prefix to account specific
    bucket_name = 'dataci-' + workspace.name
    # TODO: region should be configurable
    region = 'ap-southeast-1'
    if not fs.exists(f'/{bucket_name}'):
        create_bucket(bucket_name, region=region)

    # Mount the S3 bucket to local
    mount_bucket(bucket_name, workspace.data_dir, mount_ok=True, region=region)

    # Set the current workspace as the default workspace to config file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    config['CORE']['default_workspace'] = workspace.name
    with open(CONFIG_FILE, 'w') as f:
        config.write(f)
    logger.info(f'Set default workspace to {workspace.name} in config {CONFIG_FILE}.')
