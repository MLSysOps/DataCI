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

from dataci.config import TIMEZONE
from dataci.db.run import exist_run, create_one_run, get_next_run_version, get_latest_run_version, get_one_run, \
    patch_one_run
from dataci.models import BaseModel

if TYPE_CHECKING:
    from datetime import datetime, timezone
    from typing import Optional, Union

    from dataci.models import Workflow, Stage


class Run(BaseModel):
    # run id (uuid)
    NAME_PATTERN = re.compile(r'^[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}$', flags=re.IGNORECASE)
    VERSION_PATTERN = re.compile(r'^\d+|latest$', flags=re.IGNORECASE)
    type_name = 'run'

    def __init__(
            self,
            name: str,
            status: str,
            job: 'Union[Workflow, Stage, dict]',
            version: int,
            create_time: 'Optional[datetime]' = None,
            update_time: 'Optional[datetime]' = None,
            **kwargs
    ):
        super().__init__(name, **kwargs)
        self.status: str = status
        self._job = job
        self.version = version
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
            'job': self.job.dict(id_only=True) if self.job else None,
            'create_time': int(self.create_time.replace(tzinfo=timezone.utc).timestamp()) if self.create_time else None,
            'update_time': int(self.update_time.replace(tzinfo=timezone.utc).timestamp()) if self.update_time else None,
        }

    @classmethod
    def from_dict(cls, config):
        self = cls(**config)
        return self.reload(config)

    def reload(self, config=None):
        if config is None:
            config = get_one_run(self.name, self.version)
        self.version = config['version']
        self.create_time = datetime.fromtimestamp(config['create_time'], tz=TIMEZONE)
        self.update_time = datetime.fromtimestamp(config['update_time'], tz=TIMEZONE)

    def save(self, version=None):
        # Get next run try number
        version = self.version or get_next_run_version(self.name)
        # Check if run exists
        if exist_run(self.name, version):
            # reload
            config = get_one_run(self.name, version)
            return self.reload(config)

        config = self.dict()
        config['version'] = version
        config['update_time'] = config['create_time']
        create_one_run(config)
        return self.reload(config)

    def update(self):
        # Get latest run try number
        version = self.version or get_latest_run_version(self.name)
        # Check if run exists
        run_prev = get_one_run(self.name, version)
        if run_prev is None:
            raise ValueError(f'Run {self.name}@{version} not found.')
        # Update run by merging with previous run
        config = self.dict()
        config['version'] = version
        config['create_time'] = run_prev['create_time']
        # Overwrite with previous field values if not set
        for k, v in run_prev.items():
            if k not in config:
                config[k] = v

        patch_one_run(config)
        return self.reload(config)

    @classmethod
    def get(cls, name, version=None, not_found_ok=False):
        """Get run by name and version."""
        workspace, name, version = cls.parse_data_model_get_identifier(name, version)
        # If version not set, get the latest version
        version = version or 'latest'
        config = get_one_run(name, version)
        if config is None:
            if not_found_ok:
                return
            raise ValueError(f'Run {name}@{version} not found.')

        return cls.from_dict(config)
