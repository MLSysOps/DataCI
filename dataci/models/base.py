#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 03, 2023
"""
import abc
import re

from dataci.config import DEFAULT_WORKSPACE
from dataci.models.workspace import Workspace


class BaseModel(abc.ABC):
    NAME_PATTERN = re.compile(r'^(?:[a-z]\w*\.)?[a-z]\w*$', flags=re.IGNORECASE)
    VERSION_PATTERN = re.compile(r'latest|v\d+|none|[\da-f]+', flags=re.IGNORECASE)
    GET_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
        r'^(?:([a-z]\w*)\.)?([a-z]\w*)(?:@(latest|v\d+|none|[\da-f]+))?$', flags=re.IGNORECASE
    )
    LIST_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
        r'^(?:([a-z]\w*)\.)?([\w:.*[\]]+?)(?:@(\d+|latest|none|\*))?$', re.IGNORECASE
    )
    type_name: str

    def __init__(self, name, *args, **kwargs):
        # Prevent to pass invalid arguments to object.__init__
        mro = type(self).mro()
        for next_cls in mro[mro.index(BaseModel) + 1:]:
            if '__init__' in next_cls.__dict__:
                break
        else:
            next_cls = None

        if not next_cls == object:
            super().__init__(*args, **kwargs)
        workspace_name, name = name.split('.') if '.' in name else (None, name)
        self.workspace = Workspace(workspace_name)
        self.name = name
        # Version will be filled on save
        self.version = None
        # Version tag will be filled on publish
        self.version_tag = None

    @property
    def full_name(self):
        return f'{self.workspace.name}.{self.name}'

    @property
    def identifier(self):
        return f'{self.workspace.name}.{self.name}@{self.version}'

    @property
    def uri(self):
        return f'dataci://{self.workspace.name}/{self.type_name}/{self.name}/{self.version}'

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
    def get(cls, name, version=None, not_found_ok=False):
        pass

    # @classmethod
    # @abc.abstractmethod
    # def find(cls, identifier=None, tree_view=False):
    #     pass

    @classmethod
    def parse_data_model_get_identifier(cls, name, version=None):
        # If version is provided along with name
        matched = cls.GET_DATA_MODEL_IDENTIFIER_PATTERN.match(str(name))
        if not matched:
            raise ValueError(f'Invalid identifier {name} for get operation')
        # Parse name and version
        workspace, name, version_ = matched.groups()
        workspace = workspace or DEFAULT_WORKSPACE
        # Only one version is allowed to be provided, either in name or in version
        if version and version_:
            raise ValueError('Only one version is allowed to be provided by name or version.')

        version = version or version_
        if version:
            version = str(version).lower()

        return workspace, name, version

    @classmethod
    def parse_data_model_list_identifier(cls, identifier):
        workflow_identifier = identifier or '*'

        matched = cls.LIST_DATA_MODEL_IDENTIFIER_PATTERN.match(workflow_identifier)
        if not matched:
            raise ValueError(f'Invalid identifier {workflow_identifier} for list operation')
        workspace, name, version = matched.groups()
        workspace = workspace or DEFAULT_WORKSPACE
        # Case                      Provided        Matched    Action
        # version is not provided   ws.name         None       Get all versions
        # version is None           ws.name@None    'None'     Get version = NULL
        # version is provided       ws.name@version 'version'  Get version = version
        if version and version.lower() == 'none':
            version = None
        else:
            version = str(version or '*').lower()

        return workspace, name, version
