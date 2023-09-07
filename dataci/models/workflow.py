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
import os
import shutil
from abc import ABC
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import networkx as nx

from dataci.db.workflow import (
    create_one_workflow,
    exist_workflow_by_version,
    get_many_workflow,
    get_next_workflow_version_id, create_one_workflow_tag, get_one_workflow_by_tag,
    get_one_workflow_by_version,
)
from .base import BaseModel
from .event import Event
from .stage import Stage
from .workspace import Workspace
# from dataci.run import Run
from ..utils import hash_binary, cwd, hash_file

if TYPE_CHECKING:
    from typing import Optional, Iterable, Sequence
    from dataci.models import Dataset

logger = logging.getLogger(__name__)


class Workflow(BaseModel, ABC):
    name_arg = 'name'

    type_name = 'workflow'

    def __init__(self, *args, trigger=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.create_date: 'Optional[datetime]' = datetime.now()
        self.logger = logging.getLogger(__name__)
        self.trigger: 'Sequence[Event]' = trigger or list()
        self._script_dir = None
        self._entrypoint = None
        self._script_dir_local = None
        self._stage_script_paths = dict()
        self._init_params = (args, kwargs)

    @property
    @abc.abstractmethod
    def stages(self) -> 'Iterable[Stage]':
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def dag(self) -> 'nx.DiGraph':
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def input_datasets(self) -> 'Iterable[Dataset]':
        """Return all outside produced input datasets of the workflow."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def output_datasets(self) -> 'Iterable[Dataset]':
        raise NotImplementedError

    @property
    def script(self):
        if self._script_dir is None:
            return None
        return {
            'path': self._script_dir,
            'entrypoint': self._entrypoint,
            'local_path': self._script_dir_local,
        }

    @property
    @abc.abstractmethod
    def stage_script_paths(self):
        return self._stage_script_paths

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
                'node': {
                    v: {**k.dict(id_only=True), 'path': self.stage_script_paths[k.full_name]}
                    for k, v in stage_mapping.items()
                },
                'edge': dag_edge_list,
            },
            'script': self.script,
            'timestamp': int(self.create_date.timestamp()) if self.create_date else None,
            'trigger': [str(evt) for evt in self.trigger],
            'input_datasets': [dataset.dict(id_only=True) for dataset in self.input_datasets],
            'output_datasets': [],
        }

    @classmethod
    def from_dict(cls, config: 'dict'):
        # Build class object from script
        # TODO: make the build process more secure with sandbox / allowed safe methods
        local_dict = dict()
        with cwd(config['script']['path']):
            entry_file = Path(config['script']['entrypoint'])
            entry_module = '.'.join(entry_file.parts[:-1] + (entry_file.stem,))
            exec(
                f'import os, sys; sys.path.insert(0, os.getcwd()); from {entry_module} import *',
                local_dict, local_dict
            )
        for v in local_dict.copy().values():
            if isinstance(v, Workflow):
                self = v
                break
        else:
            raise ValueError(f'Stage not found in directory:\n{config["script"]["path"]}')
        return self.reload(config)

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
            'stages': [stage.fingerprint for stage in self.stages],
            'script_dir': hash_file(config['script']['path']),
            'entrypoint': config['script']['entrypoint'],
        }
        return hash_binary(json.dumps(fingerprint_dict, sort_keys=True).encode('utf-8'))

    def reload(self, config=None):
        """Reload the models from the updated config. If config is None, reload from the database."""
        config = config or get_one_workflow_by_version(self.workspace.name, self.name, self.fingerprint)
        if config is None:
            return self

        self.version = config['version']
        self.version_tag = config['version_tag'] if 'version_tag' in config else None
        self.create_date = datetime.fromtimestamp(config['timestamp']) if config['timestamp'] else None
        self.trigger = [Event.from_str(evt) for evt in config['trigger']]
        if 'script' in config:
            self._script_dir = config['script']['path']
            self._entrypoint = config['script']['entrypoint']
        if 'dag' in config:
            self._stage_script_paths.clear()
            # Reload stages by stage config fetched from DB
            stage_mapping = dict()
            for stage_config in config['dag']['node'].values():
                stage_full_name = f"{stage_config['workspace']}.{stage_config['name']}"
                config = Stage.get_config(f"{stage_full_name}@{stage_config['version']}")
                if config is not None:
                    stage_mapping[stage_full_name] = config
                # Update the stage script base path
                self._stage_script_paths[stage_full_name] = stage_config['path']
            for stage in self.stages:
                if stage.full_name in stage_mapping:
                    stage.reload(stage_mapping[stage.full_name])
        return self

    def save(self):
        """Save the models to the workspace."""
        version = self.fingerprint

        # Check if the stage is already saved
        if exist_workflow_by_version(self.workspace.name, self.name, version):
            # reload
            return self.reload()

        # Check if the models name is valid
        if self.NAME_PATTERN.match(f'{self.workspace.name}.{self.name}') is None:
            raise ValueError(f'Workflow name {self.workspace}.{self.name} is not valid.')
        # Save the used stages (only if the stage is not saved)
        for stage in self.stages:
            stage.save()

        # Get config after call save on all stages, since the stage version might be updated
        config = self.dict()
        config['version'] = version
        # Update create date
        config['timestamp'] = int(datetime.now().timestamp())

        # Copy the workflow code to the workspace workflow directory
        save_dir = self.workspace.workflow_dir / str(config['name']) / config['version']
        if save_dir.exists():
            shutil.rmtree(save_dir)
        # Copy the workflow script dir
        from_dir = Path(config['script']['path'])
        shutil.copytree(from_dir, save_dir)

        # Scan workflow script dir
        file_checksums, filemap = dict(), defaultdict(list)
        for abs_file_name in from_dir.glob('**/*'):
            if abs_file_name.is_dir():
                continue
            rel_file_name = abs_file_name.relative_to(from_dir).as_posix()
            file_checksums[str(rel_file_name)] = hash_file(abs_file_name)
            # Record for debugging info
            filemap[rel_file_name].append('workflow')

        # Copy the script content of each stage to the save dir
        for stage in self.stages:
            dag_stage_rel_dir = Path(self.stage_script_paths[stage.full_name])
            stage_root_dir = Path(stage.script['path'])
            for abs_file_name in Path(stage_root_dir).glob('**/*'):
                if abs_file_name.is_dir():
                    continue
                rel_file_name = str(dag_stage_rel_dir / abs_file_name.relative_to(stage_root_dir).as_posix())
                file_checksum = hash_file(abs_file_name)
                if file_checksums.get(rel_file_name, file_checksum) != file_checksum:
                    raise FileExistsError(
                        f'Stage {stage.identifier} script file {rel_file_name} has different content '
                        f'with other stages or workflow.\n'
                        f'Found conflict source file from: {filemap[rel_file_name]}\n'
                    )
                # Copy to workflow script dir with relative path
                if rel_file_name not in file_checksums:
                    shutil.copy2(abs_file_name, save_dir / rel_file_name)
                    file_checksums[rel_file_name] = file_checksum
                    filemap[rel_file_name].append(stage.identifier)

        config['script']['path'] = save_dir.as_posix()

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

        config = self.dict()
        # Since publish, we generate the latest version
        config['version_tag'] = get_next_workflow_version_id(workspace=config['workspace'], name=config['name'])
        create_one_workflow_tag(config)
        return self.reload(config)

    @classmethod
    def get(cls, name: str, version: str = None):
        """Get a models from the workspace."""
        workspace, name, version = cls.parse_data_model_get_identifier(name, version)

        if version is None or version == 'latest' or version.startswith('v'):
            # Get by tag
            config = get_one_workflow_by_tag(workspace, name, version)
        else:
            # Get by version
            if version.lower() == 'none':
                version = None
            config = get_one_workflow_by_version(workspace, name, version)
        if config is None:
            return

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

    @abc.abstractmethod
    def run(self, **kwargs):
        """Submit a triggered workflow run remotely."""
        pass

    @abc.abstractmethod
    def test(self, **kwargs):
        """Test run the workflow locally."""
        pass

    @abc.abstractmethod
    def backfill(self, **kwargs):
        """Submit a back-fill workflow run remotely."""
        pass
