import argparse

from dataci.models import Dataset


def save(args):
    """
    Publish a dataset to the repo.
    1. Generate dataset schema based on dataset files and perform schema checking
    2. copy content of every <targets>.dvc file to .dataci/dataset with versioning
    
    Command syntax:
        -n/--name: dataset name
        targets: path to dataset base directory
    """
    dataset = Dataset(
        name=args.name, dataset_files=args.targets,
    )
    dataset.save()


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


def update(args):
    repo = Repo()
    dataset = Dataset(name=args.name, repo=repo)
    dataset.update()


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI dataset')
    subparser = parser.add_subparsers()
    publish_parser = subparser.add_parser('save', help='Save dataset')
    publish_parser.add_argument(
        '-n', '--name', type=str, required=True, help='Dataset name'
    )
    publish_parser.add_argument('targets', type=str, help='Path to dataset base directory.')
    publish_parser.set_defaults(func=save)
    list_parser = subparser.add_parser('ls', help='List dataset')
    list_parser.add_argument(
        'targets', type=str, nargs='?', default=None,
        help='Dataset name with optional version and optional split information to query.'
    )
    list_parser.set_defaults(func=ls)

    update_parser = subparser.add_parser('update', help='Update dataset')
    update_parser.add_argument('-n', '--name', type=str, required=True, help='Dataset name.')
    update_parser.set_defaults(func=update)
    args_ = parser.parse_args()
    args_.func(args_)
