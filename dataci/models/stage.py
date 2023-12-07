#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import abc
import json
import logging
import shutil
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING

import networkx as nx

from dataci.db.stage import (
    create_one_stage,
    exist_stage,
    get_one_stage_by_version,
    get_one_stage_by_tag,
    get_next_stage_version_tag,
    get_many_stages, create_one_stage_tag
)
from .base import BaseModel
from .script import Script
from ..utils import hash_binary, cwd

if TYPE_CHECKING:
    from typing import Optional


class Stage(BaseModel):
    """Stage mixin class.

    Attributes:
        name_arg (str): The name of the keyword arguments that is used as ID to initialize the stage.
            If subclass does not use 'name' kwargs as the ID, it should override this attribute.
    """

    name_arg = 'name'
    type_name = 'stage'

    def __init__(self, *args, input_table=None, output_table=None, **kwargs) -> None:
        name = kwargs.get(self.name_arg, None)
        if name is None:
            raise TypeError(f'__init__() missing 1 required keyword-only argument: \'{self.name_arg}\'')
        self.input_table: dict = input_table or dict()
        self.output_table: dict = output_table or dict()
        super().__init__(name, *args, **kwargs)
        self.create_date: 'Optional[datetime]' = None
        self.logger = logging.getLogger(__name__)
        self._backend = 'airflow'
        self.params = dict()
        self._script = None

    @property
    @abc.abstractmethod
    def script(self) -> Script:
        """The script of the stage."""
        raise NotImplementedError

    @abc.abstractmethod
    def test(self, *args, **kwargs):
        """Test the stage."""
        raise NotImplementedError

    def dict(self, id_only=False):
        """Get the dict representation of the stage.

        Args:
            id_only (bool): If True, only return the id of the stage, otherwise return the full dict representation.
        """
        if id_only:
            return {
                'workspace': self.workspace.name,
                'type': self.type_name,
                'name': self.name,
                'version': self.version,
            }
        return {
            'name': self.name,
            'type': self.type_name,
            'workspace': self.workspace.name,
            'version': self.version,
            'version_tag': self.version_tag,
            'params': self.params,
            'script': self.script.dict(),
            'timestamp': int(self.create_date.timestamp()) if self.create_date else None,
        }

    @classmethod
    def from_dict(cls, config: dict):
        from dataci.decorators.base import DecoratedOperatorStageMixin

        # TODO: make the build process more secure with sandbox / allowed safe methods
        local_dict = dict()
        script_module = Script.from_dict(config['script'])
        with cwd(script_module.dir):
            entry_module, func_name = script_module.entrypoint.rsplit('.', 1)
            exec(
                f'import os, sys; sys.path.insert(0, os.getcwd()); from {entry_module} import {func_name}',
                local_dict, local_dict
            )
        for v in local_dict.copy().values():
            # Stage is instantiated by operator class / a function decorated by @stage
            if isinstance(v, DecoratedOperatorStageMixin):
                self = v
                break
        else:
            raise ValueError(f'Stage not found in directory:\n{config["script"]["path"]}')
        return self.reload(config)

    def __repr__(self):
        return f'{self.__class__.__name__}(name={self.workspace.name}.{self.name}@{self.version})'

    def reload(self, config=None):
        """Reload the stage from the config. If config is not provided, reload from the Database.
        """
        config = config or get_one_stage_by_version(self.workspace.name, self.name, self.fingerprint)
        if config is None:
            return self
        self.version = config['version']
        self.version_tag = config['version_tag']
        self.params = config['params']
        self.create_date = datetime.fromtimestamp(config['timestamp']) if config['timestamp'] else None
        # Manual set the script to the stage object, as the script is not available in the exec context
        if 'script' in config:
            self._script = Script.from_dict(config['script'])
        return self

    @property
    def fingerprint(self):
        config = self.dict()
        fingerprint_dict = {
            'workspace': config['workspace'],
            'name': config['name'],
            'params': config['params'],
            'script': config['script']['hash'],
        }
        return hash_binary(json.dumps(fingerprint_dict, sort_keys=True).encode('utf-8'))

    def save(self):
        """Save the stage to the workspace."""
        config = self.dict()
        version = self.fingerprint
        # Check if the stage is already saved
        if exist_stage(config['workspace'], config['name'], version):
            return self.reload()

        # stage is not saved
        config['version'] = version
        # Update create date
        config['timestamp'] = int(datetime.now().timestamp())

        # Save the stage script to the workspace stage directory
        save_dir = self.workspace.stage_dir / str(config['name']) / str(version)
        if save_dir.exists():
            # clear content
            [shutil.rmtree(f) if f.is_dir() else f.unlink() for f in save_dir.iterdir()]
        else:
            save_dir.mkdir(parents=True)
        for f in self.script.filelist:
            shutil.copy(self.script.dir / f, save_dir / f)

        # Update the script path in the config
        config['script']['dir'] = str(save_dir)
        create_one_stage(config)
        return self.reload(config)

    def publish(self):
        """Publish the stage to the workspace."""
        # Save the stage to the workspace first
        self.save()
        # Check if the stage is already published
        if self.version_tag is not None:
            self.logger.warning(f'Stage {self} is already published with version_tag={self.version_tag}')
            return self
        config = self.dict()
        config['version_tag'] = str(get_next_stage_version_tag(config['workspace'], config['name']))
        create_one_stage_tag(config)

        self.logger.info(f'Published stage: {self} with version_tag={config["version_tag"]}')

        return self.reload(config)

    @classmethod
    def get_config(cls, name, version=None, workspace=None):
        """Get the stage config from the workspace."""
        workspace_, name, version_or_tag = cls.parse_data_model_get_identifier(name, version)
        # Override workspace if provided
        workspace = workspace or workspace_
        if version_or_tag == 'latest' or version_or_tag.startswith('v'):
            config = get_one_stage_by_tag(workspace, name, version_or_tag)
        else:
            config = get_one_stage_by_version(workspace, name, version_or_tag)

        return config

    @classmethod
    def get(cls, name, version=None, workspace=None, not_found_ok=False):
        """Get the stage from the workspace."""
        config = cls.get_config(name, version, workspace)
        if config is None:
            if not not_found_ok:
                raise ValueError(f'Stage {name}@{version} not found')
            return

        return cls.from_dict(config)

    @classmethod
    def get_by_workflow(cls, stage_name, workflow_name, workflow_version=None):
        """Get the stage from the workspace."""
        from dataci.models.workflow import Workflow

        workflow_config = Workflow.get_config(workflow_name, workflow_version)
        if workflow_config is None:
            raise ValueError(f'Workflow {workflow_name}@{workflow_version} not found')
        # Find stage version
        for _, v in workflow_config['dag']['node'].items():
            if v['name'] == stage_name:
                stage_version = v['version']
                break
        else:
            raise ValueError(f'Stage {stage_name} not found in workflow {workflow_name}@{workflow_version}')

        return cls.get(stage_name, stage_version)

    @classmethod
    def find(cls, stage_identifier, tree_view=False, all=False):
        """Find the stage from the workspace."""
        workspace, name, version = cls.parse_data_model_list_identifier(stage_identifier)
        stage_configs = get_many_stages(workspace, name, version, all=all)
        stages = [cls.from_dict(stage_config) for stage_config in stage_configs]
        if tree_view:
            stage_dict = defaultdict(dict)
            for stage in stages:
                stage_dict[stage.full_name][stage.version] = stage
            return stage_dict

        return stages

    def upstream(self, n=1, type=None):
        """Get the downstream stages of the stage.
        TODO: type is miss-aligned with stage::upstream, only 'run' and 'dataset' are supported.
        """
        from dataci.models.run import Run

        runs = Run.find(self.full_name, job_type=self.type_name)
        graphs = list()
        node_mapping = dict()
        for run in runs:
            g = run.upstream(n=n, type=type)
            for node in g.nodes:
                if isinstance(node, Run):
                    node_mapping[node] = node._job
            nx.relabel_nodes(g, node_mapping, copy=False)
            graphs.append(g)
        # Merge all graphs
        upstream_graph = nx.compose_all(graphs)
        # Replace the node with the stage object
        node_mapping = {
            node: Stage.get(name=node['name'], version=node['version'], workspace=node['workspace'])
            for node in upstream_graph.nodes() if node['type'] == 'stage'
        }
        nx.relabel_nodes(upstream_graph, node_mapping, copy=False)
        return upstream_graph

    def downstream(self, n=1, type=None):
        """Get the downstream stages of the stage.
        """
        from dataci.models.run import Run

        runs = Run.find_by_job(workspace=self.workspace.name, name=self.name, version=self.version, type=self.type_name)
        graphs = list()
        node_mapping = dict()
        for run in runs:
            g = run.downstream(n=n, type=type)
            for node in g.nodes:
                if isinstance(node, Run):
                    node_mapping[node] = node._job
            nx.relabel_nodes(g, node_mapping, copy=False)
            graphs.append(g)
        # Merge all graphs
        downstream_graph = nx.compose_all(graphs)
        # Replace the node with the stage object
        node_mapping = {
            node: Stage.get(name=node['name'], version=node['version'], workspace=node['workspace'])
            for node in downstream_graph.nodes() if node['type'] == 'stage'
        }
        nx.relabel_nodes(downstream_graph, node_mapping, copy=False)
        return downstream_graph
