#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 09, 2023
"""
import importlib.util

import click

from dataci.models import Stage


@click.group(name='stage')
def stage():
    """DataCI Stage management."""
    pass


@stage.command()
@click.argument(
    'targets', type=str, nargs='?', default=None,
)
def publish(targets):
    """Publish a stage.

    Commands:
        targets: module_name:stage_variable. Module name to stage to be published.
            In the format of "module_name:stage_variable".
            For example, your workflow stage_1 is written at file "dir1/dir2/file.py", the target is:
            "dir1.dir2.file:stage_1".
    """
    module_name, workflow_var = targets.split(':')

    spec = importlib.util.find_spec(module_name)
    if spec is None:
        raise ValueError(f'Cannot find module with name: {module_name}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    stage = getattr(module, workflow_var, None)
    if stage is None or not isinstance(stage, Stage):
        raise ValueError(f'Cannot find stage variable: {workflow_var} at module {module_name}')
    stage.publish()
