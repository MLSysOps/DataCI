#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 03, 2023
"""
import abc

from dataci.models.workspace import Workspace


class BaseModel(abc.ABC):
    def __init__(self, name, **kwargs):
        workspace_name, name = name.split('.') if '.' in name else (None, name)
        self.workspace = Workspace(workspace_name)
        self.name = name
        # Version will be filled on publish
        self.version = None

    @property
    def full_name(self):
        return f'{self.workspace.name}.{self.name}'

    @property
    def identifier(self):
        return f'{self.workspace.name}.{self.name}@{self.version}'

    @abc.abstractmethod
    def dict(self):
        pass

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, config: dict):
        pass

    @abc.abstractmethod
    def save(self):
        pass

    @abc.abstractmethod
    def publish(self):
        pass

    @classmethod
    @abc.abstractmethod
    def get(cls, name, version=None):
        pass

    # @classmethod
    # @abc.abstractmethod
    # def find(cls, identifier=None, tree_view=False):
    #     pass
