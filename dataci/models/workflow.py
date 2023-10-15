#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 23, 2023
"""
import abc
import importlib
import itertools
import json
import logging
import multiprocessing as mp
import shutil
from abc import ABC
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import cloudpickle
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
from .script import Script
from .stage import Stage
from .workflow_patcher import patch as patch_func
# from dataci.run import Run
from ..utils import hash_binary, cwd, hash_file

if TYPE_CHECKING:
    import os
    from typing import Optional, Iterable, Sequence, Dict, Union
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
        self._script: 'Optional[Script]' = None
        self._stage_script_paths = dict()
        self._init_params = (args, kwargs)

    @property
    @abc.abstractmethod
    def stages(self) -> 'Dict[str, Stage]':
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
    @abc.abstractmethod
    def script(self) -> Script:
        raise NotImplementedError

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
            'script': self.script.dict(),
            'timestamp': int(self.create_date.timestamp()) if self.create_date else None,
            'trigger': [str(evt) for evt in self.trigger],
            'input_datasets': [dataset.dict(id_only=True) for dataset in self.input_datasets],
            'output_datasets': [],
        }

    @classmethod
    def from_dict(cls, config: 'dict'):
        # Build class object from script
        script = Script.from_dict(config['script'])
        self = cls.from_path(script.dir, script.entrypoint)
        return self.reload(config)

    @classmethod
    def from_path(cls, script_dir: 'Union[str, os.PathLike]', entry_path: 'Union[str, os.PathLike]'):
        # TODO: make the build process more secure with sandbox / allowed safe methods
        def _import_module(import_pickle):
            import os
            import cloudpickle
            import sys
            from dataci.models import Workflow
            sys.path.insert(0, os.getcwd())

            mod = importlib.import_module(entry_module)
            # get all variables from the module
            for k, v in mod.__dict__.items():
                if not k.startswith('__') and isinstance(v, Workflow):
                    import_pickle['__return__'] = cloudpickle.dumps(v)
                    break
            else:
                raise ValueError(f'Workflow not found in directory: {script_dir}')

        with cwd(script_dir):
            entry_file = Path(entry_path)
            entry_module = '.'.join(entry_file.parts[:-1] + (entry_file.stem,))
            with mp.Manager() as manager:
                import_pickle = manager.dict()
                p = mp.Process(target=_import_module, args=(import_pickle,))
                p.start()
                p.join()
                self = cloudpickle.loads(import_pickle['__return__'])

        return self

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
            'stages': [stage.fingerprint for stage in self.stages.values()],
            'script': config['script']['hash'],
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
            # fixme: reload the object if the script hash is changed
            self._script = Script.from_dict(config['script'])
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
            for stage in self.stages.values():
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
        for stage in self.stages.values():
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
        from_dir = Path(config['script']['dir'])
        for file in config['script']['filelist']:
            (save_dir / file).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(from_dir / file, save_dir / file)

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
        for stage in self.stages.values():
            if (dag_stage_rel_dir := self.stage_script_paths[stage.full_name]) is None:
                # Skip the stage if the stage script is external
                continue
            dag_stage_rel_dir = Path(dag_stage_rel_dir)
            stage_root_dir = Path(stage.script.dir)
            for abs_file_name in Path(stage_root_dir).glob('**/*'):
                if abs_file_name.is_dir():
                    continue
                rel_file_name = (dag_stage_rel_dir / abs_file_name.relative_to(stage_root_dir)).as_posix()
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

    def patch(self, verbose=True, **kwargs):
        """Patch the new stage to the same name stage in workflow."""
        assert len(kwargs) == 1, 'Only one stage can be patched at a time.'

        # Save the workflow first
        self.save()

        full_k, stage = None, None
        for k, stage in kwargs.items():
            # Convert k to full stage name
            full_k = f'{self.workspace.name}.{k}' if self.workspace else k
            # Check if the stage is in the workflow
            if full_k not in self.stages:
                raise ValueError(f'Cannot find stage name={k} in workflow {self.name}')
            # TODO: Check if the stage has the same signature
            # Warning if the new stage has different signature with the old stage
        new_workflow = patch_func(self, source_name=full_k, target=stage, logger=self.logger, verbose=verbose)

        return new_workflow

    def publish(self):
        """Publish the models to the workspace."""
        # TODO: use DB transaction / data object lock
        # Save models first
        self.save()
        # Check if the models is already published
        if self.version_tag is not None:
            return self

        # Publish the used stages
        for stage in self.stages.values():
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
