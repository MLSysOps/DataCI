import argparse

from dataci.models import Dataset


def save(args):
    """
    Save a dataset to the workspace.
    1. Generate dataset schema based on dataset files and perform schema checking
    2. Generate dataset version
    3. copy content of dataset files to workspace data storage and save dataset metadata to database
    
    Command syntax:
        -n/--name: dataset name
        targets: path to dataset base directory
    """
    dataset = Dataset(
        name=args.name, dataset_files=args.targets,
    )
    dataset.save()


def publish(args):
    """
    Publish a saved dataset.

    Command syntax:
        targets: dataset identifier, including workspace, dataset name and version.
    """
    datasets = Dataset.find(dataset_identifier=args.targets, tree_view=False, all=True)
    if len(datasets) == 0:
        raise ValueError(f'No dataset found for {args.targets}')
    if len(datasets) > 1:
        raise ValueError(
            f'Dataset identifier {args.targets} is ambiguous, '
            f'please specify one of the following using longer version ID:\n'
            "\n".join([dataset.identifier for dataset in datasets]),
        )
    dataset = datasets[0]
    dataset.publish()


def ls(args):
    dataset_version_dict = Dataset.find(dataset_identifier=args.targets)

    for dataset_name, version_dict in dataset_version_dict.items():
        print(dataset_name)
        if len(version_dict) > 0:
            print(
                f'|  Version\tYield pipeline\tParent dataset\tSize\tCreate time'
            )
        for version, dataset in version_dict.items():
            print(
                f'|- {version}\t{dataset.yield_pipeline or "N.A."}\t\t{dataset.parent_dataset or "N.A."}\t\t'
                f'{dataset.size or "N.A."}\t{dataset.create_date.strftime("%Y-%m-%d %H:%M:%S")}'
            )


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI dataset')
    subparser = parser.add_subparsers()
    save_parser = subparser.add_parser('save', help='Save dataset')
    save_parser.add_argument(
        '-n', '--name', type=str, required=True, help='Dataset name'
    )
    save_parser.add_argument('targets', type=str, help='Path to dataset base directory.')
    save_parser.set_defaults(func=save)

    list_parser = subparser.add_parser('ls', help='List dataset')
    list_parser.add_argument(
        'targets', type=str, nargs='?', default=None,
        help='Dataset name with optional version and optional split information to query.'
    )
    list_parser.set_defaults(func=ls)

    publish_parser = subparser.add_parser('publish', help='Update dataset')
    publish_parser.add_argument(
        'targets', type=str,
        help='Dataset identifier. E.g., workspace.dataset_name@saved_version'
    )
    publish_parser.set_defaults(func=publish)
    args_ = parser.parse_args()
    args_.func(args_)
