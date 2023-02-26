import argparse
import os
import shutil
import subprocess
from pathlib import Path


def run(args):
    root_path = Path(os.getcwd())
    dataci_repo_dir = root_path / '.dataci'
    if args.force:
        # Clean up
        if dataci_repo_dir.exists() and dataci_repo_dir.is_dir():
            shutil.rmtree(dataci_repo_dir)
    # Init dataci directory
    data_ci_folders = [
        dataci_repo_dir,
        dataci_repo_dir / 'dataset',
        dataci_repo_dir / 'pipeline',
        dataci_repo_dir / 'tmp',
    ]
    for folder in data_ci_folders:
        folder.mkdir(exist_ok=True)
    # Init DVC
    dvc_init_cmd = ['dvc', 'init', '--no-scm']
    if args.force:
        dvc_init_cmd.append('-f')
    subprocess.run(dvc_init_cmd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI init')
    parser.add_argument(
        '-f', '--force', action='store_true', 
        help='Remove .dataci and .dvc folders if it exists before initialization. This will '
            'clean up the repository and re-init.'
    )
    args_ = parser.parse_args()
    run(args_)
