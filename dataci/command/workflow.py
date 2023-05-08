#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 16, 2023
"""
import argparse
import importlib.util

from dataci.models import Workflow


def ls(args):
    workflow_version_dict = Workflow.find(args.targets, tree_view=True)

    for workflow_name, version_dict in workflow_version_dict.items():
        print(workflow_name)
        if len(version_dict) > 0:
            print(
                f'|  Version\tCreate time'
            )
        for version, pipeline in version_dict.items():
            print(
                f'|- {version[:7]}\t{pipeline.create_date.strftime("%Y-%m-%d %H:%M:%S")}'
            )


def publish(args):
    """Publish a workflow.

    Commands:
        targets: module_name:workflow_variable
    """
    module_name, workflow_var = args.targets.split(':')

    spec = importlib.util.find_spec(module_name)
    if spec is None:
        raise ValueError(f'Cannot find module with name: {module_name}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    workflow = getattr(module, workflow_var)
    if workflow is None or isinstance(workflow, Workflow):
        raise ValueError(f'Cannot find workflow variable: {workflow_var} at module {module_name}')
    workflow.publish()


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI Workflow')
    subparser = parser.add_subparsers()
    list_parser = subparser.add_parser('ls', help='List workflow')
    list_parser.add_argument(
        'targets', type=str, nargs='?', default=None,
        help='workflow name with optional version and optional split information to query.'
    )
    list_parser.set_defaults(func=ls)

    publish_parser = subparser.add_parser('publish', help='Publish a workflow')
    publish_parser.add_argument(
        'targets', type=str, nargs='?', default=None,
        help='Module name to workflow to be published. '
             'For example, your workflow wf_1 is written at file "dir1/dir2/file.py", the target is:'
             '"dir1.dir2.file:wf_1".'
    )
    publish_parser.set_defaults(func=publish)
    args_ = parser.parse_args()
    args_.func(args_)
