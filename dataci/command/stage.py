#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 09, 2023
"""
import argparse
import importlib.util

from dataci.models import Stage


def publish(args):
    """Publish a stage.

    Commands:
        targets: module_name:stage_variable
    """
    module_name, workflow_var = args.targets.split(':')

    spec = importlib.util.find_spec(module_name)
    if spec is None:
        raise ValueError(f'Cannot find module with name: {module_name}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    stage = getattr(module, workflow_var, None)
    if stage is None or not isinstance(stage, Stage):
        raise ValueError(f'Cannot find stage variable: {workflow_var} at module {module_name}')
    stage.publish()


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI Stage')
    subparser = parser.add_subparsers()

    publish_parser = subparser.add_parser('publish', help='Publish a stage')
    publish_parser.add_argument(
        'targets', type=str, nargs='?', default=None,
        help='Module name to stage to be published. '
             'For example, your workflow stage_1 is written at file "dir1/dir2/file.py", the target is:'
             '"dir1.dir2.file:stage_1".'
    )
    publish_parser.set_defaults(func=publish)
    args_ = parser.parse_args()
    args_.func(args_)
