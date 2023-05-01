#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 23, 2023
"""
import logging
from datetime import datetime
from typing import Optional

import networkx as nx

from dataci.workspace import Workspace
from . import WORKFLOW_CONTEXT


# from dataci.run import Run


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
        # latest is regard as this pipeline is not published
        self.is_built = (self.version != 'latest')
        self.logger = logging.getLogger(__name__)
        self.is_published = False

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
        if not nx.is_connected(self.dag):
            raise ValueError('All dag nodes should be connected.')
        return True

    def __call__(self):
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
        return {
            'name': self.name,
            'version': self.version,
            'timestamp': int(self.create_date.timestamp()) if self.create_date else None,
        }

    @classmethod
    def from_dict(cls, config: 'dict'):
        pipeline = cls(**config)
        pipeline.create_date = datetime.fromtimestamp(config['timestamp'])
        pipeline.is_published = True
        return pipeline

    def __repr__(self) -> str:
        if all((self.workspace.name, self.name)):
            return f'Workflow({self.workspace.name}.{self.name}@{self.version})'
        return f'Workflow({self.workspace.name}.{self.name}) ! Unpublished'

    def __str__(self):
        return f'{self.name}@{self.version}'

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, type(self)):
            return repr(self) == repr(__o)
        return False
