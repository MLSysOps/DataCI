#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Apr 25, 2023

CLI for dataci connector APIs.
"""
import argparse
from getpass import getpass

from dataci.connector.s3 import connect as s3_connect


def s3(args):
    """CLI for connect to S3."""
    s3_connect(
        key=args.key,
        secret=args.secret,
        endpoint_url=args.endpoint_url,
    )


class PasswordPromptAction(argparse.Action):
    """Prompt for password in command line.

    References:
        https://stackoverflow.com/a/28610617
    """

    def __init__(self,
                 option_strings,
                 dest=None,
                 nargs=0,
                 default=None,
                 required=False,
                 type=None,
                 metavar=None,
                 help=None):
        super(PasswordPromptAction, self).__init__(
            option_strings=option_strings,
            dest=dest,
            nargs=nargs,
            default=default,
            required=required,
            metavar=metavar,
            type=type,
            help=help)

    def __call__(self, parser, args, values, option_string=None):
        password = getpass(prompt='Secret:')
        if len(password) == 0:
            password = None
        setattr(args, self.dest, password)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('DataCI Cloud Service Connector')
    subparser = parser.add_subparsers()
    s3_parser = subparser.add_parser('s3', help='Connect S3')
    s3_parser.add_argument('-k', '--key', type=str, help='Access ID for S3')
    s3_parser.add_argument(
        '-p', '--secret', action=PasswordPromptAction, type=str, help='Access key for S3'
    )
    s3_parser.add_argument('-u', '--endpoint-url', type=str, help='S3 endpoint url')
    s3_parser.set_defaults(func=s3)

    args = parser.parse_args()
    args.func(args)
