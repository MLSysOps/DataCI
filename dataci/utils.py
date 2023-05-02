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

NAME_PATTERN = re.compile(r'^(?:[a-z]\w*\.)?[a-z]\w*$', flags=re.IGNORECASE)
VERSION_PATTERN = re.compile(r'latest|\d+', flags=re.IGNORECASE)
GET_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
    r'^(?:([a-z]\w*)\.)?([a-z]\w*)(?:@(latest|\d+))?$', flags=re.IGNORECASE
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
