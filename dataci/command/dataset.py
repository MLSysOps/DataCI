import argparse

from dataci.dataset import publish_dataset, list_dataset
from dataci.repo import Repo
from dataci.fs.ref import DataRef


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
    if args.targets:
        targets = DataRef(args.targets).path
    else:
        targets = dict()
        if args.train:
            targets['train'] = DataRef(args.train).path
        if args.val:
            targets['val'] = DataRef(args.val).path
        if args.test:
            targets['test'] = DataRef(args.test).path
    publish_dataset(repo=repo, dataset_name=args.name, targets=targets)


def ls(args):
    repo = Repo()
    dataset_split_version_dict = list_dataset(repo=repo, dataset_identifier=args.targets)

    for dataset_name, split_version_dict in dataset_split_version_dict.items():
        print(dataset_name)
        for sec_cnt, (split, version_dict) in enumerate(split_version_dict.items()):
            if sec_cnt != 0:
                # print a spacing between different sections
                print('|')
            print(f'| [{split}]')
            if len(version_dict) > 0:
                print(
                    f'|  Version\tYield pipeline\tParent dataset\tSize\tCreate time'
                )
            for version, dataset in version_dict.items():
                print(
                    f'|- {version[:7]}\t{dataset.yield_pipeline or "N.A."}\t\t{dataset.parent_dataset or "N.A."}\t\t'
                    f'{dataset.size or "N.A."}\t{dataset.create_date.strftime("%Y-%m-%d %H:%M:%S")}'
                )


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI dataset')
    subparser = parser.add_subparsers()
    publish_parser = subparser.add_parser('publish', help='Publish dataset')
    publish_parser.add_argument(
        '-n', '--name', type=str, required=True, help='Dataset name'
    )
    publish_parser.add_argument('targets', type=str, nargs='*', help='Path to dataset base directory.')
    publish_parser.add_argument('--train', type=str, help='Path to the train dataset base directory')
    publish_parser.add_argument('--val', type=str, help='Path to the val dataset base directory')
    publish_parser.add_argument('--test', type=str, help='Path to the test dataset base directory')
    publish_parser.set_defaults(func=publish)
    list_parser = subparser.add_parser('ls', help='List dataset')
    list_parser.add_argument(
        'targets', type=str, nargs='?', default=None,
        help='Dataset name with optional version and optional split information to query.'
    )
    list_parser.set_defaults(func=ls)
    args_ = parser.parse_args()
    args_.func(args_)
