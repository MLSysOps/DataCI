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
@click.argument('targets', type=str, required=False, default=None)
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
        for version, workflow in version_dict.items():
            create_time = workflow.create_date
            if create_time is not None:
                create_time = create_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                create_time = 'N.A.'
            click.echo(
                f'|- {(version or "")[:7]}\t{create_time}'
            )


@workflow.command()
@click.argument(
    'module_path', type=str, required=True,
)
@click.argument(
    'workflow_var', type=str, required=False,
)
def publish(module_path, workflow_var):
    """Publish a workflow.

    Commands:
        targets: module_name:workflow_variable. Module name to workflow to be published.
             For example, your workflow wf_1 is written at file "dir1/dir2/file.py", the target is:
             "dir1.dir2.file:wf_1"
    """
    if workflow_var is None:
        # module path is a workflow_identifier
        workflows = Workflow.find(module_path)
        if len(workflows) == 0:
            raise ValueError(f'Cannot find workflow with identifier: {module_path}')
        elif len(workflows) > 1:
            raise ValueError(f'Find multiple workflows with identifier.'
                             f'Please specify workflow variable name from one of them:\n'
                             + "\n".join(workflows))
        workflow_obj = workflows[0]
        click.echo(workflow_obj.publish().identifier)
        return

    module_name = f'__{workflow_var}__'
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None:
        raise ValueError(f'Cannot find module with path: {module_path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    workflow = getattr(module, workflow_var, None)
    if workflow is None or not isinstance(workflow, Workflow):
        raise ValueError(f'Cannot find workflow variable: {workflow_var} at module {module_name}')
    click.echo(workflow.publish().identifier)
