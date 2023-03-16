#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 16, 2023
"""
import argparse
from dataci.pipeline.list import list_pipeline
from dataci.repo import Repo
from dataci.run.list import list_run


def ls(args):
    repo = Repo()
    pipeline_version_dict = list_pipeline(pipeline_identifier=args.targets, repo=repo)
    pipeline_version_run_dict = list_run(pipeline_identifier=args.targets, repo=repo)

    for pipeline_name, version_dict in pipeline_version_dict.items():
        print(pipeline_name)
        if len(version_dict) > 0:
            print(
                f'|  Version\tCreate time'
            )
        for version, pipeline in version_dict.items():
            print(
                f'|- {version[:7]}\t{pipeline.create_date.strftime("%Y-%m-%d %H:%M:%S")}'
            )
            for run in pipeline_version_run_dict.get(pipeline_name, dict()).get(version, list()):
                print(
                    f'|    |- run{run.run_num}'
                )


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI pipeline')
    subparser = parser.add_subparsers()
    list_parser = subparser.add_parser('ls', help='List pipeline')
    list_parser.add_argument(
        'targets', type=str, nargs='?', default=None,
        help='Dataset name with optional version and optional split information to query.'
    )
    list_parser.set_defaults(func=ls)
    args_ = parser.parse_args()
    args_.func(args_)
