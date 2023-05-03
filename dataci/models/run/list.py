#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 16, 2023
"""
from collections import defaultdict
from typing import TYPE_CHECKING

from dataci.pipeline.list import LIST_PIPELINE_IDENTIFIER_PATTERN
from dataci.run import Run

from dataci.db.run import get_many_runs

if TYPE_CHECKING:
    from typing import Optional
    from dataci.repo import Repo


def list_run(pipeline_identifier=None, tree_view=True, repo: 'Optional[Repo]' = None):
    pipeline_identifier = pipeline_identifier or '*'
    matched = LIST_PIPELINE_IDENTIFIER_PATTERN.match(pipeline_identifier)
    if not matched:
        raise ValueError(f'Invalid pipeline identifier {pipeline_identifier}')
    pipeline_name, pipeline_version = matched.groups()
    pipeline_version = (pipeline_version or '').lower() + '*'

    # Check matched runs
    run_dict_list = get_many_runs(pipeline_name, pipeline_version)
    run_list = list()
    for run_dict in run_dict_list:
        run_dict['repo'] = repo
        run_list.append(Run.from_dict(run_dict))

    if tree_view:
        run_dict = defaultdict(lambda: defaultdict(list))
        for run in run_list:
            run_dict[run.pipeline.name][run.pipeline.version].append(run)
        return run_dict

    return run_list
