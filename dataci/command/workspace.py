import argparse

from dataci.workspace import create_or_use_workspace, Workspace


def use(args):
    """Use a workspace. If the workspace does not exist, create it."""
    workspace_name = args.name
    workspace = Workspace(workspace_name)
    create_or_use_workspace(workspace)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI use workspace')
    subparser = parser.add_subparsers()
    use_parser = subparser.add_parser('use', help='Use default workspace')
    use_parser.add_argument('name', type=str, help='Workspace name')
    use_parser.set_defaults(func=use)
    args_ = parser.parse_args()
    args_.func(args_)
