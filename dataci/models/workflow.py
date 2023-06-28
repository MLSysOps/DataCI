#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 23, 2023
"""
import abc
import itertools
import json
import logging
import re
from abc import ABC
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING

import networkx as nx

from dataci.db.workflow import (
    create_one_workflow,
    exist_workflow_by_tag,
    exist_workflow_by_version,
    update_one_workflow,
    get_one_workflow,
    get_many_workflow,
    get_next_workflow_version_id, get_workflow_tag_or_none, create_one_workflow_tag,
)
from dataci.decorators.event import event
from .base import BaseModel
from .stage import Stage
# from dataci.run import Run
from ..utils import hash_binary

if TYPE_CHECKING:
    from typing import Optional, Iterable

logger = logging.getLogger(__name__)


class Workflow(BaseModel, ABC):
    GET_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
        r'^(?:([a-z]\w*)\.)?([a-z]\w*)(?:@(latest|[a-f\d]{32}|\d+))?$', flags=re.IGNORECASE
    )
    LIST_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
        r'^(?:([a-z]\w*)\.)?([\w:.*[\]]+?)(?:@(\d+|latest|[a-f\d]{1,32}|\*))?$', re.IGNORECASE
    )
    name_arg = 'name'

    def __init__(self, *args, **kwargs):
        if len(args) > 0:
            name = args[0]
        else:
            name = kwargs.get(self.name_arg)
        super().__init__(name, *args, **kwargs)
        self.create_date: 'Optional[datetime]' = datetime.now()
        self.logger = logging.getLogger(__name__)
        self._script = None
        self._init_params = (args, kwargs)
        # set during runtime
        self._input_dataset = list()
        self._output_dataset = list()

    @property
    @abc.abstractmethod
    def stages(self) -> 'Iterable[Stage]':
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def dag(self) -> 'nx.DiGraph':
        raise NotImplementedError

    @property
    def script(self):
        return self._script

    def dict(self, id_only=False):
        if id_only:
            return {'workspace': self.workspace.name, 'name': self.name, 'version': self.version}
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
        # Also add other nodes
        for stage in self.dag.nodes:
            _ = stage_mapping[stage]

        return {
            'workspace': self.workspace.name,
            'name': self.name,
            'version': self.version,
            'dag': {
                'node': {v: k.dict(id_only=True) for k, v in stage_mapping.items()},
                'edge': dag_edge_list,
            },
            'script': self.script,
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

    @property
    def fingerprint(self):
        config = self.dict()
        fingerprint_dict = {
            'workspace': config['workspace'],
            'name': config['name'],
            'script': config['script'],
            'stages': [stage.fingerprint for stage in self.stages],
        }
        return hash_binary(json.dumps(fingerprint_dict, sort_keys=True).encode('utf-8'))

    def reload(self, config):
        """Reload the models from the updated config."""
        self.version = config['version'] if config['version'] != 'head' else None
        self.create_date = datetime.fromtimestamp(config['timestamp']) if config['timestamp'] else None
        return self

    @event(name='workflow_save')
    def save(self):
        """Save the models to the workspace."""
        config = self.dict()
        version = self.fingerprint

        # Check if the stage is already saved
        if exist_workflow_by_version(config['workspace'], config['name'], version):
            self.version = version
            # Get stage tag
            version_tag = get_workflow_tag_or_none(self.workspace.name, self.name, version)
            self.version_tag = version_tag
            return self

        # Check if the models name is valid
        if self.NAME_PATTERN.match(f'{self.workspace.name}.{self.name}') is None:
            raise ValueError(f'Workflow name {self.workspace}.{self.name} is not valid.')
        # Save the used stages (only if the stage is not saved)
        for stage in self.stages:
            stage.save()
            logger.info(f'Saved stage: {stage}')

        config['version'] = version
        # Update create date
        config['timestamp'] = int(datetime.now().timestamp())
        # Save the workflow
        create_one_workflow(config)
        return self.reload(config)

    # def patch(self, stage: Stage):
    #     """Patch the new stage to the same name stage in workflow."""
    #     patch_mapping = dict()
    #     for s in self.stages:
    #         if s.full_name == stage.full_name:
    #             patch_mapping[s] = stage
    #
    #     # Relabel the graph
    #     self.dag = nx.relabel_nodes(self.dag, patch_mapping, copy=True)
    #     return self

    @event(name='workflow_publish')
    def publish(self):
        """Publish the models to the workspace."""
        # TODO: use DB transaction / data object lock
        # Save models first
        self.save()
        # Check if the models is already published
        if self.version_tag is not None:
            return self

        # Publish the used stages
        for stage in self.stages:
            stage.publish()
            logger.info(f'Published stage: {stage}')

        config = self.dict()
        # Since publish, we generate the latest version
        config['version_tag'] = get_next_workflow_version_id(workspace=config['workspace'], name=config['name'])
        create_one_workflow_tag(config)
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
