#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import inspect
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from dataci.db.stage import create_one_stage, exist_stage, update_one_stage
from dataci.workspace import Workspace
from . import WORKFLOW_CONTEXT

if TYPE_CHECKING:
    from networkx import DiGraph

logger = logging.getLogger(__name__)


class Stage(ABC):
    def __init__(
            self, name: str, symbolize: str = None, **kwargs,
    ) -> None:
        workspace_name, task_name = name.split('.') if '.' in name else (None, name)
        self.workspace = Workspace(workspace_name)
        self.name = task_name
        self.symbolize = symbolize
        # Version is automatically generated when the stage is published
        self.version = None
        # Output is saved after the stage is run, this is for the descendant stages to use
        self._output = None

    @abstractmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError('Method `run` not implemented.')

    @property
    def script(self):
        # Get the source code of the class
        try:
            source_code = inspect.getsource(self.__class__)
        except OSError:
            # If the source code is not available, the class is probably dynamically generated
            # We can get the source code from the wrapped function
            source_code = inspect.getsource(getattr(self, '__wrapped__'))
        return source_code

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
            'script': self.script,
            'cls_name': self.__class__.__name__,
            'symbolize': self.symbolize,
        }

    @classmethod
    def from_dict(cls, config: dict):
        config['name'] = f'{config["workspace"]}.{config["name"]}'
        # Build class object from script
        # TODO: make the build process more secure with sandbox / allowed safe methods
        local_dict = locals()
        exec(config['script'], globals(), local_dict)
        sub_cls = local_dict[config['cls_name']]
        self = sub_cls(**config)
        return self

    def add_downstream(self, stage: 'Stage'):
        dag: DiGraph = self.context.get('dag')
        if dag is None:
            raise ValueError('DAG is not set in context.')
        dag.add_edge(self, stage)

        return stage

    @property
    def context(self):
        # Get context from contextvars, this will be set within the context of a workflow
        return WORKFLOW_CONTEXT.get()

    @property
    def ancestors(self):
        return self.context.get('dag').predecessors(self)

    def __call__(self, *args, **kwargs):
        # Inspect run method signature
        sig = inspect.signature(self.run)
        # If context is in run method signature, pass context to run method
        if 'context' in sig.parameters:
            kwargs.update(self.context)
        outputs = self.run(*args, **kwargs)
        self._output = outputs
        return outputs

    def __rshift__(self, other):
        return self.add_downstream(other)

    def __repr__(self):
        return f'{self.__class__.__name__}(name={self.workspace.name}.{self.name})'

    def save(self):
        """Save the stage to the workspace."""
        config = self.dict()
        # Since save, we set the version to head (this is different from latest)
        config['version'] = 'head'

        # Save the stage script to the workspace stage directory
        save_dir = self.workspace.stage_dir / config['name'] / config['version']
        save_file_path = save_dir / 'script.py'
        save_dir.mkdir(parents=True, exist_ok=True)

        # Update the script path in the config
        config['script_path'] = str(save_file_path.relative_to(self.workspace.stage_dir))
        if not exist_stage(config['workspace'], config['name'], config['version']):
            with open(save_file_path, 'w') as f:
                f.write(config['script'])
            create_one_stage(config)
        else:
            with open(save_file_path, 'w') as f:
                f.write(config['script'])
            update_one_stage(config)
        return self
