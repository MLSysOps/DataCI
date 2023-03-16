#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 16, 2023
"""
from collections import defaultdict
from typing import TYPE_CHECKING

from dataci.db.run import get_many_runs
from dataci.run import Run

if TYPE_CHECKING:
    from typing import Optional
    from dataci.repo import Repo


def list_run(pipeline_name, pipeline_version=None, tree_view=True, repo: 'Optional[Repo]' = None):
    pipeline_version = (pipeline_version or '') + '*'
    run_dict_list = get_many_runs(pipeline_name, pipeline_version)
    run_list = list()
    for run_dict in run_dict_list:
        run_dict['repo'] = repo
        run_list.append(Run.from_dict(run_dict))

    if tree_view:
        run_dict = defaultdict(dict)
        for run in run_list:
            run_dict[run.pipeline.name][run.pipeline.version] = run
        return run_dict

    return run_list
