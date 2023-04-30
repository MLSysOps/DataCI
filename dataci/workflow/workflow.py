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
from .stage import VirtualStage


# from dataci.run import Run


class Workflow(object):
    from .publish import publish  # type: ignore[misc]
    from .get import get_next_run_num  # type: ignore[misc]

    def __init__(
            self,
            name: str,
            params: dict = None,
            **kwargs,
    ):
        workspace_name, name = name.split('.') if '.' in name else (None, name)
        self.name = name
        self.workspace = Workspace(workspace_name)
        # Context for each stage
        self.params = params or dict()
        self.dag = nx.DiGraph()
        # Add a root node to dag
        self.root_stage = VirtualStage('__root')
        self.dag.add_node(self.root_stage)
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
        }

    @property
    def stages(self):
        return self.dag.nodes

    # def build(self):
    #     self.workdir.mkdir(exist_ok=True, parents=True)
    #     (self.workdir / self.CODE_DIR).mkdir(exist_ok=True)
    #     (self.workdir / self.FEAT_DIR).mkdir(exist_ok=True)
    #
    #     with cwd(self.workdir):
    #         for stage in self.stages:
    #             # For each stage
    #             # resolve input and output feature path
    #             stage.code_base_dir = Path(self.CODE_DIR)
    #             stage.feat_base_dir = Path(self.FEAT_DIR)
    #
    #             # Pack the stage object and all its dependencies to `code_dir`
    #             file_dict = stage.serialize()
    #             for file_path, file_bytes in file_dict.items():
    #                 with open(file_path, 'wb') as f:
    #                     f.write(file_bytes)
    #
    #             # Get output path
    #             output_path = str(stage.outputs.dataset_files if isinstance(stage.outputs, Dataset) else stage.outputs)
    #
    #             # manage stages by dvc
    #             # dvc stage add -n <stage name> -d stage.py -d input.csv -O output.csv -w self.workdir python stage.py
    #             cmd = [
    #                 'dvc', 'stage', 'add', '-f', '-n', str(stage.name),
    #                 '-o', output_path, '-w', str(self.workdir),
    #             ]
    #             # Add dependencies
    #             for dependency in stage.dependency:
    #                 if isinstance(dependency, Dataset):
    #                     # Link global dataset files path to local
    #                     local_file_path = os.path.join(self.FEAT_DIR, dependency.name + dependency.dataset_files.suffix)
    #                     symlink_force(dependency.dataset_files, local_file_path)
    #                     dependency = local_file_path
    #                 else:
    #                     dependency = os.path.relpath(str(dependency), str(self.workdir))
    #                 cmd += ['-d', dependency]
    #             # Add running command
    #             cmd += ['python', os.path.join(self.CODE_DIR, f'{stage.name}.py')]
    #             subprocess.call(cmd)
    #         # Get pipeline version
    #         self.version = generate_pipeline_version_id(self.CODE_DIR)
    #     self.is_built = True

    def validate(self):
        """
        Validate the workflow:
        1. there is any cycle in the dag
        2. All stages have an ancestor except the root stage
        """
        if not nx.is_directed_acyclic_graph(self.dag):
            raise ValueError('The dag should be a directed acyclic graph.')
        if not all(nx.ancestors(self.dag, stage) for stage in self.stages if stage != self.root_stage):
            raise ValueError(
                'All stages should have an ancestor except the root stage.\n'
                'Add the root stage of the workflow to current stages by `workflow.root_stage >> stage`.\n'
            )
        return True

    def __call__(self):
        # Validate the workflow
        self.validate()

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

        return run

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
