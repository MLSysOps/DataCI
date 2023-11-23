#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023

Run for pipeline.
"""
import os
from copy import deepcopy
from shutil import rmtree, copy2
from typing import TYPE_CHECKING

from dataci.utils import symlink_force

if TYPE_CHECKING:
    from typing import Union

    from dataci.models import Workflow, Stage


class Run(object):
    def __init__(self, run_id: str, status: str, job: 'Union[Workflow, Stage, dict]', try_num: int, **kwargs):
        self.run_id = run_id
        self.status: str = status
        self._job = job
        self.try_num = try_num

    @property
    def job(self) -> 'Union[Workflow, Stage]':
        """Lazy load job (workflow or stage) from database."""
        from dataci.models import Workflow, Stage

        if not isinstance(self._job, (Workflow, Stage)):
            job_id = self._job['workspace'] + '.' + self._job['name'] + '@' + self._job['version']
            if self._job['type'] == 'workflow':
                self._job = Workflow.get(job_id)
            elif self._job['type'] == 'stage':
                self._job = Stage(self._job)
            else:
                raise ValueError(f'Invalid job type: {self._job}')
        return self._job

    def dict(self, id_only=False):
        if id_only:
            return {
                'run_id': self.run_id,
            }
        return {
            'run_id': self.run_id,
            'status': self.status,
            'job': self.job.dict(id_only=True),
            'try_num': self.try_num,
        }

    @classmethod
    def from_dict(cls, config):
        return cls(**config)
