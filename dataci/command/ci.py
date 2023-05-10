#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 10, 2023
"""
import argparse

import yaml

from dataci.action.ci_builder import build


def publish(args):
    """Publish CI/CD workflow from YAML file"""
    with open(args.targets, 'r') as f:
        config = yaml.safe_load(f)
    build(config)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI CI/CD Workflow')
    subparser = parser.add_subparsers()

    publish_parser = subparser.add_parser('publish', help='Publish a CI/CD workflow')
    publish_parser.add_argument(
        'targets', type=str, nargs='?', default=None,
        help='Path to CI/CD YAML configuration file.'
    )
    publish_parser.set_defaults(func=publish)
    args_ = parser.parse_args()
    args_.func(args_)
