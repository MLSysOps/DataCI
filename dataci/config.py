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
from threading import Event as ThreadEvent

import requests


def init():
    """
    Init dataci configuration
    """
    CONFIG_FILE.parent.mkdir(exist_ok=True)
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
# Storage backend
storage_backend = {storage_backend}
""".format(
            workspace='default',
            db_file=str(CACHE_ROOT / 'dataci.db'),
            log_dir=str(CACHE_ROOT / 'log'),
            log_level='INFO',
            storage_backend='local',
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
    global DEFAULT_WORKSPACE, DB_FILE, LOG_DIR, LOG_LEVEL, STORAGE_BACKEND

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    DEFAULT_WORKSPACE = config.get('CORE', 'default_workspace', fallback=None)
    DB_FILE = Path(config.get('CORE', 'db_file', fallback=''))
    LOG_DIR = Path(config.get('CORE', 'log_dir', fallback=''))
    LOG_LEVEL = config.get('CORE', 'log_level', fallback=None)
    STORAGE_BACKEND = config.get('CORE', 'storage_backend', fallback='local')

    # Configure the logging
    # TODO: log to file
    logging.basicConfig(level=LOG_LEVEL)
    logging.info(f'Load configuration from {CONFIG_FILE}.')

    # Try if server is available
    try:
        r = requests.get(f'http://{SERVER_ADDRESS}:{SERVER_PORT}/live')
        if r.status_code == 200:
            DISABLE_EVENT.clear()
            logging.debug(f'DataCI server is live: {SERVER_ADDRESS}:{SERVER_PORT}')
        logging.debug(f'DataCI server response: {r.status_code}')
    except requests.exceptions.ConnectionError:
        logging.warning(
            f'DataCI server is not live: {SERVER_ADDRESS}:{SERVER_PORT}. Event trigger is disabled.'
        )
        DISABLE_EVENT.set()


CACHE_ROOT = Path(os.environ.get('DATACI_CACHE_ROOT', Path.home() / '.dataci'))
CONFIG_FILE = CACHE_ROOT / 'config.ini'
DEFAULT_WORKSPACE = None
DB_FILE = None
LOG_DIR = None
LOG_LEVEL = None
STORAGE_BACKEND = None

# DataCI Trigger and Scheduler server
SERVER_ADDRESS = '0.0.0.0'
SERVER_PORT = 8000
DISABLE_EVENT = ThreadEvent()

# Helper for disable dag build in `exec`
DISABLE_WORKFLOW_BUILD = ThreadEvent()

load_config()
