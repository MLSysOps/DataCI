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

from dataci.workspace import Workspace

logger = logging.getLogger(__name__)


class Stage(ABC):
    def __init__(
            self, name: str, symbolize: str = None, **kwargs,
    ) -> None:
        workspace_name, task_name = name.split('.') if '.' in name else (None, name)
        self.workspace = Workspace(workspace_name)
        self.name = task_name
        self.symbolize = symbolize
        # context is set by workflow
        self.context = dict()

    @abstractmethod
    def run(self, inputs):
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

    def dict(self):
        return dict(
            name=self.name,
            workspace=self.workspace.name,
            script=self.script,
            symbolize=self.symbolize,
            cls_name=self.__class__.__name__,
        )

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

    def __call__(self, inputs=None):
        kwargs = dict()
        if inputs is not None:
            kwargs.update(inputs)
        # Inspect run method signature
        sig = inspect.signature(self.run)
        # If context is in run method signature, pass context to run method
        if 'context' in sig.parameters:
            kwargs.update(self.context)
        outputs = self.run(**kwargs)
        return outputs
