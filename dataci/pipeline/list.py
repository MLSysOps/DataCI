#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 08, 2023
"""
import re
from typing import TYPE_CHECKING

from dataci.pipeline import Pipeline

if TYPE_CHECKING:
    from dataci.repo import Repo

LIST_PIPELINE_IDENTIFIER_PATTERN = re.compile(
    r'^([\w.*[\]]+?)(?:@([\da-f]{1,40}))?(?:/([1-9.*[\]][\d.*[\]]*))?$', re.IGNORECASE
)


def ls(repo: Repo, pipeline_identifier):
    """List pipeline with optional pipeline identifier to query.

    Args:
        pipeline_identifier (str): Pipeline identifier. Default to query all pipelines.

    TODO: run number is not used
    """
    pipeline_identifier = pipeline_identifier or '*'
    matched = LIST_PIPELINE_IDENTIFIER_PATTERN.match(pipeline_identifier)
    if not matched:
        raise ValueError(f'Invalid dataset identifier {pipeline_identifier}')
    pipeline_name, version, run_num = matched.groups()
    version = (version or '').lower() + '*'
    run_num = run_num or '*'

    # Check matched pipeline
    pipeline_names = list()
    for folder in repo.pipeline_dir.glob(pipeline_name):
        if folder.is_dir() and not folder.is_symlink():
            pipeline_names.append(folder.name)

    pipelines = list()
    for pipeline_name in pipeline_names:
        for ver in (repo.pipeline_dir / pipeline_name).glob(version):
            if ver.is_dir() and not ver.is_symlink():
                pipeline = Pipeline(name=pipeline_name, version=ver, basedir=repo.pipeline_dir)
                pipeline.restore()
                pipelines.append(pipeline)
    return pipelines
