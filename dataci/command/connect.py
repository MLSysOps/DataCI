#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Apr 25, 2023

CLI for dataci connector APIs.
"""
import click


@click.group()
def connect():
    """Connect to cloud services."""
    pass


@connect.command()
@click.option('-k', '--key', type=str, help='Access Key ID for S3')
@click.password_option(
    '-p', '--secret', prompt=True, hide_input=True,
    confirmation_prompt=False, help='Access key Secret for S3'
)
@click.option('-u', '--endpoint-url', type=str, help='S3 endpoint url')
def s3(key, secret, endpoint_url):
    """CLI for connect to S3."""
    from dataci.connector.s3 import connect as s3_connect

    s3_connect(
        key=key,
        secret=secret,
        endpoint_url=endpoint_url,
    )
