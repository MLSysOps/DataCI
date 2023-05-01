#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 01, 2023
"""
import configparser
import logging
import os
from pathlib import Path


def init():
    """
    Init dataci configuration
    """

    with CONFIG_FILE.open('w') as f:
        f.write("""[CORE]
# The database file
db_file = {db_file}
# The log directory
log_dir = {log_dir}
# The default log level
log_level = {log_level}
# The default workspace
default_workspace = {workspace}
""".format(
            workspace='default',
            db_file=str(CACHE_ROOT / 'dataci.db'),
            log_dir=str(CACHE_ROOT / 'log'),
            log_level='INFO',
        ))
    logging.info(f'Create configuration file {CONFIG_FILE}.')

    CONFIG_FILE.chmod(0o600)
    CACHE_ROOT.mkdir(exist_ok=True)

    # Reload the configuration
    load_config()

    # Create the DB file
    from dataci.db import init as init_db  # noqa
    logging.info('Init database.')


def load_config():
    """
    Load dataci configuration
    """
    # export the configuration to global variables
    global DEFAULT_WORKSPACE, DB_FILE, LOG_DIR, LOG_LEVEL

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    DEFAULT_WORKSPACE = config.get('CORE', 'default_workspace', fallback=None)
    DB_FILE = Path(config.get('CORE', 'db_file', fallback=''))
    LOG_DIR = Path(config.get('CORE', 'log_dir', fallback=''))
    LOG_LEVEL = config.get('CORE', 'log_level', fallback=None)

    # Configure the logging
    # TODO: log to file
    logging.basicConfig(level=LOG_LEVEL)
    logging.info(f'Load configuration from {CONFIG_FILE}.')


CACHE_ROOT = Path(os.environ.get('DATACI_CACHE_ROOT', Path.home() / '.dataci'))
CONFIG_FILE = CACHE_ROOT / 'config.ini'
DEFAULT_WORKSPACE = None
DB_FILE = None
LOG_DIR = None
LOG_LEVEL = None

load_config()
