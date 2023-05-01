#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 23, 2023
"""
import itertools
import logging
from collections import defaultdict
from datetime import datetime
from typing import Optional

import networkx as nx

from dataci.workspace import Workspace
from . import WORKFLOW_CONTEXT
from .stage import Stage
# from dataci.run import Run
from ..db.workflow import create_one_workflow, exist_workflow, update_one_workflow

logger = logging.getLogger(__name__)


class Workflow(object):
    from .publish import publish  # type: ignore[misc]
    from .get import get_next_run_num  # type: ignore[misc]

    def __init__(
            self,
            name: str,
            params: dict = None,
            debug: bool = True,
            **kwargs,
    ):
        workspace_name, name = name.split('.') if '.' in name else (None, name)
        self.name = name
        self.workspace = Workspace(workspace_name)
        # Context for each stage
        self.params = params or dict()
        self.flag = {'debug': debug}
        self.dag = nx.DiGraph()
        self.context_token = None
        self.version = None
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
        Validate the workflow:
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
            # Validate the workflow
            self.validate()

            # Execute the workflow from the root stage
            stages = nx.topological_sort(self.dag)
            # Skip the root stage, since it is a virtual stage
            for stage in stages:
                self.logger.info(f'Executing stage: {stage}')
                inputs = [t._output for t in stage.ancestors if t._output is not None]
                stage(*inputs)
                self.logger.info(f'Finished stage: {stage}')

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
        return {
            'workspace': self.workspace.name,
            'name': self.name,
            'version': self.version,
            'params': self.params,
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
        stage_mapping = {k: Stage.get(**v) for k, v in config['dag']['node'].items()}
        # Convert the dag edge list
        dag_edge_list = [
            (stage_mapping[source], stage_mapping[target], data) for source, target, data in dag_edge_list
        ]
        # Build the workflow
        workflow = cls(config['name'], params=config['params'], **config['flag'])
        workflow.dag.add_edges_from(dag_edge_list)
        workflow.version = config['version']
        workflow.create_date = datetime.fromtimestamp(config['timestamp']) if config['timestamp'] else None
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
        """Reload the workflow from the updated config."""
        self.version = config['version'] if config['version'] != 'head' else None
        self.create_date = datetime.fromtimestamp(config['timestamp']) if config['timestamp'] else None
        return self

    def save(self):
        """Save the workflow to the workspace."""
        # Save the used stages (only if the stage is not saved)
        for stage in self.stages:
            if stage.version is None:
                stage.save()
                logger.info(f'Saved stage: {stage}')

        config = self.dict()
        # Since save, we set the version to head (this is different from latest)
        config['version'] = 'head'
        # Set the dag node (stage) version to head
        for stage_dict in config['dag']['node'].values():
            stage_dict['version'] = 'head'
        # Update create date
        config['timestamp'] = int(datetime.now().timestamp())
        # Save the workflow
        if not exist_workflow(config['workspace'], config['name'], config['version']):
            create_one_workflow(config)
            logger.info(f'Saved workflow: {self}')
        else:
            update_one_workflow(config)
            logger.info(f'Updated workflow: {self}')
        return self.reload(config)
