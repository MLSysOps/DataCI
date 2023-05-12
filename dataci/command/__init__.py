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
    dataset, stage, workflow, workspace, connect, ci
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
    from dataci.server.main import main

    main()


@cli.command()
def standalone():
    """Initialize and start all DataCI services."""
    from dataci.config import init as init_config
    from dataci.connector.s3 import connect as s3_connect
    from dataci.models import Workspace
    from dataci.server.main import main

    click.echo('Initializing DataCI...')
    init_config()

    click.echo('Connect to AWS S3...')
    key = click.prompt(
        'AWS Access Key ID', type=str, default='', show_default=False, confirmation_prompt=False
    )
    secret = click.prompt(
        'AWS Access Key Secret', type=str, default='', show_default=False, hide_input=True, confirmation_prompt=False
    )
    endpoint_url = click.prompt(
        'AWS S3 Endpoint URL', type=str, default='', show_default=False, confirmation_prompt=False
    )
    key = key if key else None
    secret = secret if secret else None
    endpoint_url = endpoint_url if endpoint_url else None
    s3_connect(
        key=key,
        secret=secret,
        endpoint_url=endpoint_url,
    )

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
cli.add_command(ci.ci)

if __name__ == '__main__':
    cli()
