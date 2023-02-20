import argparse
import logging
import os
import subprocess
import time
from pathlib import Path

import yaml


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
    dataci_repo_dir = Path(os.getcwd()) / '.dataci'
    dataset_name = args.name
    targets = Path(args.targets)
    
    # check dataset splits
    splits = list()
    for split_dir in os.scandir(targets):
        if split_dir.is_dir():
            splits.append(split_dir.name)
    for split in splits:
        if split not in ['train', 'val', 'test']:
            raise ValueError(f'{split} is not a valid split name. Expected "train", "val", "test".')
    targets = [(targets / split).resolve() for split in splits]

    # Data file version controled by DVC
    logging.info(f'Caching dataset files: {targets}')
    subprocess.run(['dvc', 'add'] + targets)
    
    # Save tracked dataset to repo
    repo_dataset_path = dataci_repo_dir / 'dataset' / dataset_name
    repo_dataset_path.mkdir(exist_ok=True)
    # Patch meta data to each generated .dvc file
    # FIXME: hard coded version, need generate version
    meta = {'version': '1', 'timestamp': int(time.time()), 'output_pipeline': []}
    for target in targets:
        dvc_filename = target.with_suffix('.dvc')
        with open(dvc_filename, 'r') as f:
            dvc_config = yaml.safe_load(f)
        dvc_config['meta'] = meta
        # Save tracked dataset to repo
        dataset_tracked_file = repo_dataset_path / (dvc_filename.stem + ".yaml")
        print(dataset_tracked_file)
        logging.info(f'Adding meta data: {dataset_tracked_file}')
        with open(dataset_tracked_file, 'a') as f:
            yaml.safe_dump({meta['version']: dvc_config}, f)


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
