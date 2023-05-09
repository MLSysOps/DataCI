import argparse

from dataci.models import Workspace


def use(args):
    """Use a workspace. If the workspace does not exist, create it."""
    workspace_name = args.name
    workspace = Workspace(workspace_name)
    workspace.use()


def rm(args):
    """Remove a workspace."""
    workspace_name = args.name
    workspace = Workspace(workspace_name)
    workspace.remove()


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI use workspace')
    subparser = parser.add_subparsers()
    use_parser = subparser.add_parser('use', help='Use default workspace')
    use_parser.add_argument('name', type=str, help='Workspace name')
    use_parser.set_defaults(func=use)

    rm_parser = subparser.add_parser('rm', help='Remove workspace')
    rm_parser.add_argument('name', type=str, help='Workspace name')
    rm_parser.set_defaults(func=rm)

    args_ = parser.parse_args()
    args_.func(args_)
