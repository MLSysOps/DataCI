#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""

import click

import dataci
from dataci.command import (
    dataset, stage, workflow, workspace, connect
)


@click.group()
@click.version_option(dataci.__version__)
def cli():
    pass


@cli.command()
def init():
    """DataCI initialization."""
    from dataci.config import init as init_config

    init_config()


@cli.command()
def start():
    """Start DataCI server."""
    from dataci.server.server import main

    main()


@cli.command()
def standalone():
    """Initialize and start all DataCI services."""
    from dataci.config import init as init_config
    from dataci.models import Workspace
    from dataci.server.server import main

    click.echo('Initializing DataCI...')
    init_config()

    click.echo('Create a default workspace...')
    workspace_name = click.prompt(
        'Workspace name', type=str, default='testspace', show_default=True, confirmation_prompt=False)
    Workspace(workspace_name).use()

    click.echo('Start DataCI server...')
    main()


cli.add_command(dataset.dataset)
cli.add_command(stage.stage)
cli.add_command(workflow.workflow)
cli.add_command(workspace.workspace)
cli.add_command(connect.connect)

if __name__ == '__main__':
    cli()
