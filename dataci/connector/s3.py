#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Apr 25, 2023
"""
import configparser

import s3fs

from dataci import CONFIG_FILE


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
    key = key or s3_config.get('key', '')
    secret = secret or s3_config.get('secret', '')
    endpoint_url = endpoint_url or s3_config.get('endpoint_url', '')

    # replace empty string "" with None, since config ini file will need save None as "".
    key_ = None if not key else key
    secret_ = None if not secret else secret
    endpoint_url_ = None if not endpoint_url else endpoint_url

    # test connection
    s3 = s3fs.S3FileSystem(anon=False, key=key_, secret=secret_, endpoint_url=endpoint_url_)
    s3.ls('/')

    # save credentials to config file
    config.set(section_name, 'key', key)
    config.set(section_name, 'secret', secret)
    config.set(section_name, 'endpoint_url', endpoint_url)
    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)


if __name__ == '__main__':
    connect()
