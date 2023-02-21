import argparse

from dataci.dataset import publish_dataset
from dataci.repo import Repo


def publish(args):
    """
    Publish a dataset to the repo.
    1. Generate dataset schema based on dataset files and perform schema checking
    1. dvc add targets with DataCI added meta (versions, create date)
    3. copy content of every <targets>.dvc file to .dataci/dataset with versioning
    
    Command syntax:
        -n/--name: dataset name
        targets: path to dataset base directory, contains splits as sub-directory. Example:
            dataset
            |- train
            |- val
    """
    repo = Repo()
    publish_dataset(repo=repo, dataset_name=args.name, targets=args.targets)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI dataset')
    subparser = parser.add_subparsers()
    publish_parser = subparser.add_parser('publish', help='Publish dataset')
    publish_parser.add_argument(
       '-n', '--name', type=str, required=True, help='Dataset name'
    )
    publish_parser.add_argument('targets', type=str, help='Path to dataset base directory.')
    publish_parser.set_defaults(func=publish)
    args_ = parser.parse_args()
    args_.func(args_)
