import click

from dataci.models import Workspace


@click.group()
def workspace():
    """Manage workspaces."""
    pass


@workspace.command()
@click.argument('name')
def use(name):
    """Use a workspace. If the workspace does not exist, create it."""
    workspace_name = name
    workspace = Workspace(workspace_name)
    workspace.use()


@workspace.command()
@click.argument('name')
def rm(name):
    """Remove a workspace."""
    workspace_name = name
    workspace = Workspace(workspace_name)
    workspace.remove()
