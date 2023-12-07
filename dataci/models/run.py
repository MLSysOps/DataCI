#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023

Run for pipeline.
"""
import re
import warnings
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import networkx as nx

from dataci.config import TIMEZONE
from dataci.db.run import (
    exist_run,
    create_one_run,
    get_next_run_version,
    get_latest_run_version,
    get_one_run,
    list_run_by_job,
    update_one_run
)
from dataci.models import BaseModel
from dataci.models.lineage import LineageGraph

if TYPE_CHECKING:
    from typing import Optional, Union

    from dataci.models import Workflow, Stage


class Run(BaseModel):
    # run id (uuid)
    NAME_PATTERN = re.compile(r'^[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}$', flags=re.IGNORECASE)
    VERSION_PATTERN = re.compile(r'^\d+|latest$', flags=re.IGNORECASE)
    GET_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
        r'^(?:([a-z]\w*)\.)?([a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12})(\d+|latest$)?$', flags=re.IGNORECASE
    )
    type_name = 'run'

    def __init__(
            self,
            name: str,
            status: str,
            job: 'Union[Workflow, Stage, dict]',
            create_time: 'Optional[datetime]' = None,
            update_time: 'Optional[datetime]' = None,
            **kwargs
    ):
        super().__init__(name, **kwargs)
        self.status: str = status
        self._job = job
        self.version = None
        self.create_time = create_time
        self.update_time = update_time

    @property
    def try_num(self):
        return self.version

    @property
    def job(self) -> 'Union[Workflow, Stage]':
        """Lazy load job (workflow or stage) from database."""
        from dataci.models import Workflow, Stage
        from dataci.decorators.base import DecoratedOperatorStageMixin

        if not isinstance(self._job, (Workflow, Stage, DecoratedOperatorStageMixin)):
            workflow_id = self._job['workspace'] + '.' + self._job['name'] + '@' + self._job['version']
            if self._job['type'] == 'workflow':
                self._job = Workflow.get(workflow_id)
            elif self._job['type'] == 'stage':
                self._job = Stage.get_by_workflow(self._job['stage_name'], workflow_id)
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
            'type': self.type_name,
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
        self.status = config['status']
        self.create_time = datetime.fromtimestamp(config['create_time'], tz=TIMEZONE)
        self.update_time = datetime.fromtimestamp(config['update_time'], tz=TIMEZONE)
        return self

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

    def publish(self):
        warnings.warn('Run.publish(...) is not implemented. Use Run.save() instead.', DeprecationWarning)
        return self

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

        update_one_run(config)
        return self.reload(config)

    @classmethod
    def get(cls, name, version=None, workspace=None, not_found_ok=False):
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

    @classmethod
    def find_by_job(cls, workspace, name, version, type):
        """Find run by job id."""
        configs = list_run_by_job(workspace=workspace, name=name, version=version, type=type)

        return [cls.from_dict(config) for config in configs]

    def upstream(self, n=1, type=None):
        """Get upstream lineage."""
        g = LineageGraph.upstream(self, n, type)
        node_mapping = {node.id: node for node in g.nodes()}
        for node in g.nodes():
            if node['type'] == 'run':
                node_mapping[node] = Run.get(name=node['name'], version=node['version'])
        nx.relabel_nodes(g, node_mapping, copy=False)
        return g

    def downstream(self, n=1, type=None):
        """Get downstream lineage."""
        g = LineageGraph.downstream(self, n, type)
        node_mapping = {node.id: node for node in g.nodes()}
        for node in g.nodes():
            if node['type'] == 'run':
                node_mapping[node] = Run.get(name=node['name'], version=node['version'])
        nx.relabel_nodes(g, node_mapping, copy=False)
        return g
