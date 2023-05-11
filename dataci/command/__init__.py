#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import click

from dataci.command import (
    init, dataset, stage, workflow, workspace, connect, ci
)


@click.group()
def cli():
    pass


if __name__ == '__main__':
    cli.add_command(init.init)
    cli.add_command(dataset.dataset)
    cli.add_command(stage.stage)
    cli.add_command(workflow.workflow)
    cli.add_command(workspace.workspace)
    cli.add_command(connect.connect)
    cli.add_command(ci.ci)
    cli()
