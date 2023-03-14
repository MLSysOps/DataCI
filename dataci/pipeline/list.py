#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 08, 2023
"""
import re
from collections import defaultdict
from typing import TYPE_CHECKING

from dataci.db.pipeline import get_one_pipeline, get_many_pipeline
from dataci.pipeline import Pipeline

if TYPE_CHECKING:
    from typing import Optional
    from dataci.repo import Repo

LIST_PIPELINE_IDENTIFIER_PATTERN = re.compile(
    r'^([\w.*[\]]+?)(?:@([\da-f]{1,40}))?$', re.IGNORECASE
)

LIST_FEAT_IDENTIFIER_PATTERN = re.compile(
    r'^([1-9.*[\]]\d*):([\w.]+?)$', re.IGNORECASE
)


def get_pipeline(name, version=None, repo: 'Optional[Repo]' = None):
    version = version or 'latest'
    if version != 'latest':
        # Version hash ID should provide 7 - 40 digits
        assert 40 >= len(version) >= 7, \
            'You should provided the length of version ID within 7 - 40 (both included).'
    pipeline_dict = get_one_pipeline(name=name, version=version)
    pipeline_dict['repo'] = repo
    return Pipeline.from_dict(pipeline_dict)


def list_pipeline(pipeline_identifier, tree_view=True, repo: 'Optional[Repo]' = None):
    """List pipeline with optional pipeline identifier to query.

    Args:
        pipeline_identifier (str): Pipeline identifier. Default to query all pipelines.
    """
    pipeline_identifier = pipeline_identifier or '*'
    matched = LIST_PIPELINE_IDENTIFIER_PATTERN.match(pipeline_identifier)
    if not matched:
        raise ValueError(f'Invalid pipeline identifier {pipeline_identifier}')
    pipeline_name, version = matched.groups()
    version = (version or '').lower() + '*'

    # Check matched pipeline
    get_many_pipeline(name=pipeline_name, version=version)
    pipeline_dict_list = get_many_pipeline(name=pipeline_name, version=version)
    pipeline_list = list()
    for pipeline_dict in pipeline_dict_list:
        pipeline_dict['repo'] = repo
        pipeline_list.append(Pipeline.from_dict(pipeline_dict))
    if tree_view:
        pipeline_dict = defaultdict(dict)
        for pipeline in pipeline_list:
            pipeline_dict[pipeline.name][pipeline.version] = pipeline
        return pipeline_dict

    return pipeline_list
