#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Apr 25, 2023

CLI for dataci connector APIs.
"""
import click

from dataci.connector.s3 import connect as s3_connect


@click.group()
def connect():
    """Connect to cloud services."""
    pass


@connect.command()
@click.option('-k', '--key', type=str, help='Access Key ID for S3')
@click.password_option(
    '-p', '--secret', prompt=True, hide_input=True,
    confirmation_prompt=True, help='Access key Secret for S3'
)
@click.option('-u', '--endpoint-url', type=str, help='S3 endpoint url')
def s3(args):
    """CLI for connect to S3."""
    s3_connect(
        key=args.key,
        secret=args.secret,
        endpoint_url=args.endpoint_url,
    )
