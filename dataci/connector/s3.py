#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Apr 25, 2023
"""
import configparser
import logging
import os
import subprocess

import boto3 as boto3
import s3fs

from dataci.config import CONFIG_FILE, CACHE_ROOT

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


def create_bucket(bucket_name: str, region: str = None):
    """Create a new S3 bucket.

    Args:
        bucket_name: Name of the bucket to be created.
        region: Region to create the bucket in. If not set then the default region for DataCI (ap-southeast-1)
            will be used.
    """
    # Read AWS credentials from config file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    section_name = 'connector.s3'
    key = config.get(section_name, 'key')
    secret = config.get(section_name, 'secret')

    # Create an S3 client
    if region is None:
        s3 = boto3.client('s3', aws_access_key_id=key, aws_secret_access_key=secret)
    else:
        s3 = boto3.client(
            's3', aws_access_key_id=key, aws_secret_access_key=secret, region_name=region
        )

    # Create the bucket
    try:
        if region is None:
            s3.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        logging.info(f"Bucket {bucket_name} created successfully.")
    except Exception as e:
        logging.error(f"Error creating bucket {bucket_name}: {e}")
    finally:
        s3.close()


def mount_bucket(bucket_name: str, local_dir: str, mount_ok: bool = False, region: str = None):
    """Mount the bucket to local data directory
    """
    mounted = os.path.ismount(local_dir)
    if not mount_ok and mounted:
        raise FileExistsError(f'Local directory {local_dir} is already mounted.')
    # Step 1: check if the bucket is already mounted
    if mounted:
        logger.info(f'Bucket {bucket_name} is already mounted to local data directory {local_dir}')
    else:
        # Step 2: mount the bucket to local data directory
        cmd = [
            's3fs', f'{bucket_name}', f'{local_dir}', '-o', f'passwd_file={CACHE_ROOT / ".passwd-s3fs"}',
            '-o', f'url=https://s3-{region}.amazonaws.com',
        ]
        logger.info(f'Running command: {" ".join(cmd)}')
        subprocess.run(cmd, check=True)
        logger.info(f'Mounting S3 bucket {bucket_name} to local data directory {local_dir}')


def unmount_bucket(local_dir: str, unmount_ok: bool = False):
    """Unmount the bucket from local data directory
    """
    mounted = os.path.ismount(local_dir)
    if not unmount_ok and not mounted:
        raise FileNotFoundError(f'Local directory {local_dir} is not mounted.')
    # Step 1: check if the bucket is already mounted
    if not mounted:
        logger.info(f'Local data directory {local_dir} is not mounted.')
    else:
        # Step 2: unmount the bucket from local data directory
        cmd = ['umount', str(local_dir)]
        subprocess.run(cmd, check=True)
        logger.info(f'Unmounting S3 bucket from local data directory {local_dir}')


def download(remote_path: str, local_path: str):
    """Download a file from S3 bucket to local file system.
    """
    # Create an S3 client
    fs = connect()

    # Copy files to local
    try:
        fs.get(remote_path, local_path)
        logger.info(f"File {remote_path} copy to {local_path} successfully.")
    except Exception as e:
        logger.error(f"Error copy file {remote_path}: {e}")


if __name__ == '__main__':
    connect()
