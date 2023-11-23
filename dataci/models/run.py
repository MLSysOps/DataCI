#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023

Run for pipeline.
"""
import re
from typing import TYPE_CHECKING

from dataci.models import BaseModel

if TYPE_CHECKING:
    from datetime import datetime
    from typing import Union

    from dataci.models import Workflow, Stage


class Run(BaseModel):
    # run id (uuid)
    NAME_PATTERN = re.compile(r'^[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}$', flags=re.IGNORECASE)
    VERSION_PATTERN = re.compile(r'^\d+$', flags=re.IGNORECASE)
    type_name = 'run'

    def __init__(
            self,
            name: str,
            status: str,
            job: 'Union[Workflow, Stage, dict]',
            try_num: int,
            create_time: 'datetime',
            update_time: 'datetime',
            **kwargs
    ):
        super().__init__(name, **kwargs)
        self.status: str = status
        self._job = job
        self.version = try_num
        self.create_time = create_time
        self.update_time = update_time

    @property
    def try_num(self):
        return self.version

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
                'workspace': self.workspace.name,
                'name': self.name,
                'version': self.version,
                'type': self.type_name,
            }
        return {
            'workspace': self.workspace.name,
            'name': self.name,
            'version': self.version,
            'status': self.status,
            'job': self.job.dict(id_only=True),
            'create_time': int(self.create_time.timestamp()),
            'update_time': int(self.update_time.timestamp()),
        }

    @classmethod
    def from_dict(cls, config):
        return cls(**config)

    def save(self, version=None):
        # Get next run try number
        version = self.version or get_next_run_version(name)
        # Check if run exists
        if exist_run_by_version(self.name, version):
            # update run
            return self.update()
        create_one_run(self.dict())

    def update(self):
        pass

    @classmethod
    def get(cls, name, version=None, not_found_ok=False):
        """Get run by name and version."""
        workspace, name, version = cls.parse_data_model_get_identifier(name, version)
        config = get_run_by_uuid(name)

        return cls.from_dict(config)
