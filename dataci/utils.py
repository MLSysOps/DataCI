#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 15, 2023
"""
import os
import re
from contextlib import contextmanager

from dataci.config import DEFAULT_WORKSPACE

NAME_PATTERN = re.compile(r'^(?:[a-z]\w*\.)?[a-z]\w*$', flags=re.IGNORECASE)
VERSION_PATTERN = re.compile(r'latest|none|\d+', flags=re.IGNORECASE)
GET_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
    r'^(?:([a-z]\w*)\.)?([a-z]\w*)(?:@(latest|none|\d+))?$', flags=re.IGNORECASE
)
LIST_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
    r'^(?:([a-z]\w*)\.)?([\w:.*[\]]+?)(?:@(\d+|latest|none))?$', re.IGNORECASE
)


@contextmanager
def cwd(path):
    oldpwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpwd)


def symlink_force(target, link_name, target_is_directory=False):
    """Force to create a symbolic link"""
    try:
        os.unlink(link_name)
    except FileNotFoundError:
        pass
    os.symlink(target, link_name, target_is_directory)


def parse_data_model_get_identifier(name, version=None):
    # If version is provided along with name
    matched = GET_DATA_MODEL_IDENTIFIER_PATTERN.match(str(name))
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
        if version == 'none':
            version = None

    return workspace, name, version


def parse_data_model_list_identifier(identifier):
    workflow_identifier = identifier or '*'

    matched = LIST_DATA_MODEL_IDENTIFIER_PATTERN.match(workflow_identifier)
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
