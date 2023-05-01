#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 01, 2023
"""
import argparse

from dataci.config import init as init_config


def init(args):
    """CLI for init."""
    init_config()


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI initialization')
    subparser = parser.add_subparsers()
    init_parser = subparser.add_parser('init', help='Init DataCI')
    init_parser.set_defaults(func=init)

    args_ = parser.parse_args()
    args_.func(args_)
