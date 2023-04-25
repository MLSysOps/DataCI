#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Apr 25, 2023
"""
import configparser
import logging

import boto3 as boto3
import s3fs

from dataci import CONFIG_FILE, CACHE_ROOT

logger = logging.getLogger(__name__)


def connect(key=None, secret=None, endpoint_url=None):
    """Connect to S3 using access ID and access key.
    When connecting, make sure that the user has the right to read and write to the bucket.
    If the access ID and access key are not provided, it will read the local configuration file
    `~/.dataci/config.ini` to get the credentials.

    After authentication, the provided credentials will be cached in the local config  file `~/.dataci/config.ini`.
    The local configuration file will be used for subsequent connections.
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    section_name = 'connector.s3'
    if section_name not in config:
        config.add_section(section_name)
    s3_config = config[section_name]
    # copy original user provided credentials
    key_original, secret_original, endpoint_url_original = key, secret, endpoint_url

    key = key or s3_config.get('key', '')
    secret = secret or s3_config.get('secret', '')
    endpoint_url = endpoint_url or s3_config.get('endpoint_url', '')

    # replace empty string "" with None, since config ini file will need save None as "".
    key_ = None if not key else key
    secret_ = None if not secret else secret
    endpoint_url_ = None if not endpoint_url else endpoint_url

    # test connection
    fs = s3fs.S3FileSystem(anon=False, key=key_, secret=secret_, endpoint_url=endpoint_url_)
    fs.ls('/')

    logger.info('S3 connection established.')

    if any(map(lambda x: x is not None, [key_original, secret_original, endpoint_url_original])):
        # if any of the credentials is provided, then we need to save the credentials to config file.
        # save credentials to config file
        config.set(section_name, 'key', key)
        config.set(section_name, 'secret', secret)
        config.set(section_name, 'endpoint_url', endpoint_url)
        with open(CONFIG_FILE, 'w') as configfile:
            config.write(configfile)
        # Also save to ~/.dataci/.passwd-s3fs
        s3fs_password_file = CACHE_ROOT / '.passwd-s3fs'
        # make sure the file is readable only by the owner
        s3fs_password_file.touch(exist_ok=True, mode=0o600)
        with open(s3fs_password_file, 'w') as f:
            f.write(f'{key}:{secret}')
        logger.info('S3 credentials saved to config file.')

    return fs


def create_bucket(bucket_name: str, region: str = 'ap-southeast-1'):
    """Create a new S3 bucket.

    Args:
        bucket_name: Name of the bucket to be created.
        region: Region to create the bucket in. If not set then the default region for DataCI (ap-southeast-1)
            will be used.

    TODO:
        - Add support for other regions, better save/read the region in the config file.
    """
    # Read AWS credentials from config file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    section_name = 'connector.s3'
    key = config.get(section_name, 'key')
    secret = config.get(section_name, 'secret')

    # Create an S3 client
    s3 = boto3.client(
        's3', aws_access_key_id=key, aws_secret_access_key=secret, region_name=region
    )

    # Create the bucket
    try:
        location = {'LocationConstraint': region}
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        logging.info(f"Bucket {bucket_name} created successfully.")
    except Exception as e:
        logging.error(f"Error creating bucket {bucket_name}: {e}")


if __name__ == '__main__':
    connect()
