#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 10, 2023
"""
import click
import yaml

from dataci.action.ci_builder import build


@click.group()
def ci():
    """DataCI CI/CD Workflow management."""
    pass


@ci.command()
@click.argument(
    'targets', type=str, default=None,
)
def publish(targets):
    """Publish CI/CD workflow from YAML file

    Commands:
        targets: Path to CI/CD YAML configuration file.
    """
    with open(targets, 'r') as f:
        config = yaml.safe_load(f)
    build(config)
