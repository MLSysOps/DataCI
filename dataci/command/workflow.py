#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 16, 2023
"""
import importlib.util

import click

from dataci.models import Workflow


@click.group()
def workflow():
    """Workflow management"""
    pass


@workflow.command()
@click.argument('targets', type=str, nargs='?', default=None)
def ls(targets):
    """List workflow.

    Commands:
        targets: workflow name with optional version and optional split information to query.
    """
    workflow_version_dict = Workflow.find(targets, tree_view=True)

    for workflow_name, version_dict in workflow_version_dict.items():
        click.echo(workflow_name)
        if len(version_dict) > 0:
            click.echo(
                f'|  Version\tCreate time'
            )
        for version, pipeline in version_dict.items():
            click.echo(
                f'|- {version[:7]}\t{pipeline.create_date.strftime("%Y-%m-%d %H:%M:%S")}'
            )


@workflow.command()
@click.argument(
    'targets', type=str, nargs='?', default=None
)
def publish(targets):
    """Publish a workflow.

    Commands:
        targets: module_name:workflow_variable. Module name to workflow to be published.
             For example, your workflow wf_1 is written at file "dir1/dir2/file.py", the target is:
             "dir1.dir2.file:wf_1"
    """
    module_name, workflow_var = targets.split(':')

    spec = importlib.util.find_spec(module_name)
    if spec is None:
        raise ValueError(f'Cannot find module with name: {module_name}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    workflow = getattr(module, workflow_var, None)
    if workflow is None or not isinstance(workflow, Workflow):
        raise ValueError(f'Cannot find workflow variable: {workflow_var} at module {module_name}')
    click.echo(workflow.publish())
