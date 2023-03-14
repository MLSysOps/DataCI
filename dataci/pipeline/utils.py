#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 25, 2023
"""
import hashlib
import os
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def cwd(path):
    oldpwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpwd)


def generate_pipeline_version_id(pipeline_code_dir, log_message=None, parent_version=None):
    # TODO: Change the version ID generating logic: https://www.quora.com/How-are-Git-commit-IDs-generated
    # Find .dvc traced data files
    pipeline_code_dir = Path(pipeline_code_dir)
    log_message = log_message or ''
    parent_version = parent_version or ''

    pipeline_code_obj = b''
    for pipeline_code_file in pipeline_code_dir.glob('**/*.*'):
        if pipeline_code_file.is_file():
            with open(pipeline_code_file, 'rb') as f:
                pipeline_code_obj += str(pipeline_code_file.relative_to(pipeline_code_dir)).encode() + f.read()
    log_message_obj = log_message.encode()
    parent_version_obj = parent_version.encode()

    packed_obj = pipeline_code_obj + log_message_obj + parent_version_obj
    return hashlib.sha1(packed_obj).hexdigest()


def symlink_force(target, link_name, target_is_directory=False):
    """Force to create a symbolic link"""
    try:
        os.unlink(link_name)
    except FileNotFoundError:
        pass
    os.symlink(target, link_name, target_is_directory)
