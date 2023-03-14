#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 14, 2023
"""
from typing import TYPE_CHECKING

from dataci.db.pipeline import get_one_pipeline
from dataci.db.run import get_next_run_num as get_next_run

if TYPE_CHECKING:
    from typing import Optional
    from dataci.repo import Repo
    from dataci.pipeline import Pipeline


def get_pipeline(name, version=None, repo: 'Optional[Repo]' = None):
    from .pipeline import Pipeline

    version = version or 'latest'
    if version != 'latest':
        # Version hash ID should provide 7 - 40 digits
        assert 40 >= len(version) >= 7, \
            'You should provided the length of version ID within 7 - 40 (both included).'
    pipeline_dict = get_one_pipeline(name=name, version=version)
    pipeline_dict['repo'] = repo
    return Pipeline.from_dict(pipeline_dict)


def get_next_run_num(pipeline: 'Pipeline' = ...):
    return get_next_run(pipeline.name, pipeline.version)
