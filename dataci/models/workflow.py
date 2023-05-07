#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 23, 2023
"""
import itertools
import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING

import networkx as nx

from dataci.db.workflow import (
    create_one_workflow,
    exist_workflow,
    update_one_workflow,
    get_one_workflow,
    get_many_workflow,
    get_next_workflow_version_id,
)
from dataci.decorators.event import event
from . import WORKFLOW_CONTEXT
from .base import BaseModel
from .stage import Stage
# from dataci.run import Run
from ..utils import hash_binary

if TYPE_CHECKING:
    from typing import List, Optional

logger = logging.getLogger(__name__)


class Workflow(BaseModel):
    GET_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
        r'^(?:([a-z]\w*)\.)?([a-z]\w*)(?:@(latest|[a-f\d]{32}|\d+))?$', flags=re.IGNORECASE
    )
    LIST_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
        r'^(?:([a-z]\w*)\.)?([\w:.*[\]]+?)(?:@(\d+|latest|[a-f\d]{1,32}))?$', re.IGNORECASE
    )

    def __init__(
            self,
            name: str,
            params: dict = None,
            debug: bool = True,
            schedule: 'List[str]' = None,
            **kwargs,
    ):
        super().__init__(name, **kwargs)
        # Context for each stage
        self.params = params or dict()
        self.flag = {'debug': debug}
        # schedule: list of event expression
        # e.g., ['@daily', '@event producer name status']
        if isinstance(schedule, str):
            schedule = [schedule]
        self.schedule = schedule or list()
        self.dag = nx.DiGraph()
        self.context_token = None
        self.create_date: 'Optional[datetime]' = datetime.now()
        self.logger = logging.getLogger(__name__)

    @property
    def context(self):
        return {
            'params': self.params,
            'dag': self.dag,
            'flag': self.flag,
        }

    @property
    def stages(self):
        return self.dag.nodes

    def validate(self):
        """
        Validate the models:
        1. there is any cycle in the dag
        2. All stages are connected
        """
        if not nx.is_directed_acyclic_graph(self.dag):
            raise ValueError('The dag should be a directed acyclic graph.')
        if not nx.is_connected(nx.to_undirected(self.dag)):
            raise ValueError('All dag nodes should be connected.')
        return True

    def __call__(self):
        with self:
            # Validate the models
            self.validate()

            # Execute the models from the root stage
            stages = nx.topological_sort(self.dag)
            # Skip the root stage, since it is a virtual stage
            for stage in stages:
                self.logger.info(f'Executing stage: {stage}')
                inputs = [t._output for t in stage.ancestors if t._output is not None]
                stage(*inputs)
                self.logger.info(f'Finished stage: {stage}')
            # Get the output of the last stage
            # TODO: support multiple outputs
            return stages[-1]._output

        # # Create a Run
        # run = Run(pipeline=self, run_num=self.get_next_run_num())
        # run.prepare()
        # with cwd(run.workdir):
        #     # dvc repo
        #     cmd = ['dvc', 'repro', str(run.workdir / 'dvc.yaml')]
        #     subprocess.call(cmd)
        #     if auto_save and self.is_published:
        #         run.save()
        #         self.logger.info(
        #             f'{",".join(map(str, self.inputs))} '
        #             f'>>> {str(run)} '
        #             f'>>> {",".join(map(str, self.outputs))}')
        #
        # return run

    def __enter__(self):
        self.context_token = WORKFLOW_CONTEXT.set(self.context)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        WORKFLOW_CONTEXT.reset(self.context_token)

    def dict(self):
        # export the dag as a dict
        # 1. convert the dag to a list of edges
        # 2. convert each node from Stage to an id
        dag_edge_list = nx.to_edgelist(self.dag)
        new_id = itertools.count()
        # Build stage conversion mapping
        stage_mapping = defaultdict(new_id.__next__)
        # Convert the dag edge list
        dag_edge_list = [
            (stage_mapping[source], stage_mapping[target], data) for source, target, data in dag_edge_list
        ]

        # Translate the schedule list to a list of event string
        schedule_list = list()
        for e in self.schedule:
            if e.startswith('@event'):
                # @event producer name status -> producer:name:status
                schedule_list.append(':'.join(e.split(' ')[1:]))
            else:
                raise ValueError(f'Invalid event expression: {e}')
        return {
            'workspace': self.workspace.name,
            'name': self.name,
            'version': self.version,
            'params': self.params,
            'schedule': schedule_list,
            'dag': {
                'node': {v: k.dict(id_only=True) for k, v in stage_mapping.items()},
                'edge': dag_edge_list,
            },
            'flag': self.flag,
            'timestamp': int(self.create_date.timestamp()) if self.create_date else None,
        }

    @classmethod
    def from_dict(cls, config: 'dict'):
        # 1. convert the dag to a list of edges
        # 2. convert each node from Stage to an id
        dag_edge_list = config['dag']['edge']
        # Build stage conversion mapping
        stage_mapping = {
            k: Stage.get(f"{v['workspace']}.{v['name']}", version=v['version'])
            for k, v in config['dag']['node'].items()
        }
        # Convert the dag edge list
        dag_edge_list = [
            (stage_mapping[source], stage_mapping[target], data) for source, target, data in dag_edge_list
        ]
        # Translate the schedule list to a list of event string
        for i, e in enumerate(config['schedule']):
            # producer:name:status -> @event producer name status
            config['schedule'][i] = '@event ' + ' '.join(e.split(':'))
        # Build the models
        workflow = cls(config['name'], params=config['params'], schedule=config['schedule'], **config['flag'])
        workflow.dag.add_edges_from(dag_edge_list)
        workflow.reload(config)
        return workflow

    def __repr__(self) -> str:
        if all((self.workspace.name, self.name)):
            return f'Workflow({self.workspace.name}.{self.name}@{self.version})'
        return f'Workflow({self.workspace.name}.{self.name} ! Unpublished)'

    def __str__(self):
        return f'Workflow({self.workspace.name}.{self.name}@{self.version})'

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, type(self)):
            return repr(self) == repr(__o)
        return False

    def reload(self, config):
        """Reload the models from the updated config."""
        self.version = config['version'] if config['version'] != 'head' else None
        self.create_date = datetime.fromtimestamp(config['timestamp']) if config['timestamp'] else None
        return self

    @event(name='workflow_save')
    def save(self):
        """Save the models to the workspace."""
        # Check if the models name is valid
        if self.NAME_PATTERN.match(f'{self.workspace.name}.{self.name}') is None:
            raise ValueError(f'Workflow name {self.workspace}.{self.name} is not valid.')
        # Save the used stages (only if the stage is not saved)
        for stage in self.stages:
            if stage.version is None:
                stage.save()
                logger.info(f'Saved stage: {stage}')

        config = self.dict()
        # Since save, we force set the version to None (this is different from latest)
        config['version'] = None
        # Update create date
        config['timestamp'] = int(datetime.now().timestamp())
        # Save the models
        if not exist_workflow(config['workspace'], config['name'], config['version']):
            create_one_workflow(config)
            logger.info(f'Saved models: {self}')
        else:
            update_one_workflow(config)
            logger.info(f'Updated models: {self}')
        return self.reload(config)

    def cache(self):
        """Cache the models to the workspace with a version. This is a temp function for CI.

        Refer to how to make DAG versioning: https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-36+DAG+Versioning
        """
        config = self.dict()
        # We generate the fingerprint for the workflow
        version_generate_config = {
            'workspace': config['workspace'],
            'name': config['name'],
            'dag': config['dag'],
            'schedule': config['schedule'],
            'params': config['params'],
            'flag': config['flag'],
        }
        fingerprint_bin = json.dumps(version_generate_config, sort_keys=True).encode('utf-8')
        config['version'] = hash_binary(fingerprint_bin)
        create_one_workflow(config)
        self.reload(config)
        logger.info(f'Cached models: {self}')
        return self

    @event(name='workflow_publish')
    def publish(self):
        """Publish the models to the workspace."""
        # TODO: use DB transaction / data object lock
        # Save models first
        self.save()
        # Publish the used stages (only if the stage is not published)
        for stage in self.stages:
            if stage.version is None:
                stage.publish()
                logger.info(f'Published stage: {stage}')

        config = self.dict()
        # Since publish, we generate the latest version
        config['version'] = get_next_workflow_version_id(workspace=config['workspace'], name=config['name'])
        create_one_workflow(config)
        return self.reload(config)

    @classmethod
    def get(cls, name: str, version: str = None):
        """Get a models from the workspace."""
        workspace, name, version = cls.parse_data_model_get_identifier(name, version)

        config = get_one_workflow(workspace, name, version)
        return cls.from_dict(config)

    @classmethod
    def find(cls, workflow_identifier: str = None, tree_view: bool = False):
        workspace, name, version = cls.parse_data_model_list_identifier(workflow_identifier)

        # Check matched pipeline
        workflow_dict_list = get_many_workflow(workspace, name, version)
        workflow_list = list()
        for workflow_dict in workflow_dict_list:
            workflow_list.append(cls.from_dict(workflow_dict))
        if tree_view:
            workflow_dict = defaultdict(dict)
            for workflow in workflow_list:
                workflow_dict[workflow.full_name][workflow.version] = workflow
            return workflow_dict

        return workflow_list
