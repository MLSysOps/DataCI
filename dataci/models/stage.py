#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import abc
import inspect
import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING

from dataci.db.stage import (
    create_one_stage,
    exist_stage,
    get_one_stage_by_version,
    get_one_stage_by_tag,
    get_next_stage_version_tag,
    get_many_stages, create_one_stage_tag
)
from .base import BaseModel
from .workspace import Workspace
from ..utils import hash_binary

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
    def script(self):
        # Note: if the Stage object is created from exec, it is not able to get the source code by this function
        # Therefore, we need to manual set the script passed to exec to self._script
        if self._script is None:
            # Get the source code of the class
            try:
                source_code = inspect.getsource(self.__class__)
            except OSError:
                # If the source code is not available, the class is probably dynamically generated
                # We can get the source code from the wrapped function
                source_code = inspect.getsource(getattr(self, '__wrapped__'))
            self._script = source_code
        return self._script

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
                'name': self.name,
                'version': self.version,
            }
        return {
            'name': self.name,
            'workspace': self.workspace.name,
            'version': self.version,
            'version_tag': self.version_tag,
            'params': self.params,
            'script': self.script,
            'timestamp': self.create_date.timestamp() if self.create_date else None,
        }

    @classmethod
    def from_dict(cls, config: dict):
        from dataci.decorators.base import DecoratedOperatorStageMixin

        # Get script from stage code directory
        workspace = Workspace(config['workspace'])
        with open(workspace.stage_dir / config['script_path']) as f:
            script = f.read()

        config['script'] = script
        # Build class object from script
        # TODO: make the build process more secure with sandbox / allowed safe methods
        local_dict = locals()
        # Disable the workflow build in script
        script = 'import dataci\ndataci.config.DISABLE_WORKFLOW_BUILD.set()\n' + script
        exec(script, None, local_dict)
        for v in local_dict.copy().values():
            # Stage is instantiated by operator class / a function decorated by @stage
            if isinstance(v, (Stage, DecoratedOperatorStageMixin)) and \
                    v.full_name == f'{config["workspace"]}.{config["name"]}':
                self = v
                break
        else:
            raise ValueError(f'Stage not found by script: {config["script_path"]}\n{script}')

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
            self._script = config['script']
        return self

    @property
    def fingerprint(self):
        config = self.dict()
        fingerprint_dict = {
            'workspace': config['workspace'],
            'name': config['name'],
            'params': config['params'],
            'script': config['script'],
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
        save_dir = self.workspace.stage_dir / str(config['name'])
        save_file_path = (save_dir / config['version']).with_suffix('.py')
        save_dir.mkdir(parents=True, exist_ok=True)

        # Update the script path in the config
        config['script_path'] = str(save_file_path.relative_to(self.workspace.stage_dir))
        with open(save_file_path, 'w') as f:
            f.write(config['script'])
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
    def get(cls, name, version=None):
        """Get the stage from the workspace."""
        workspace, name, version_or_tag = cls.parse_data_model_get_identifier(name, version)
        if version_or_tag == 'latest' or version_or_tag.startswith('v'):
            config = get_one_stage_by_tag(workspace, name, version_or_tag)
        else:
            config = get_one_stage_by_version(workspace, name, version_or_tag)
        stage = cls.from_dict(config)
        return stage

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
