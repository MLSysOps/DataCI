#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 25, 2023
"""
import hashlib
from pathlib import Path


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
