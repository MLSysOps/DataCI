#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 02, 2023

Event for triggers.
"""
import logging
import time

import requests

from dataci.config import SERVER_ADDRESS, SERVER_PORT

logger = logging.getLogger(__name__)


class Event(object):
    def __init__(self, name, producer, status=None):
        self.name = name
        self.producer = producer
        self.status = status
        self.timestamp = time.time()
        self.server_address = SERVER_ADDRESS
        self.server_port = SERVER_PORT
        # Try if server is available
        self._server_live = False
        try:
            r = requests.get(f'http://{self.server_address}:{self.server_port}/live')
            if r.status_code == 200:
                self._server_live = True
                logger.info(f'DataCI server is live: {self.server_address}:{self.server_port}')
            logger.debug(f'DataCI server response: {r.status_code}')
        except requests.exceptions.ConnectionError:
            logger.error(f'DataCI server is not live: {self.server_address}:{self.server_port}')

    def __repr__(self):
        return f'<Event {self.producer}:{self.name}:{self.status}>'

    def start(self):
        self.status = 'start'
        self._set()

    def success(self):
        self.status = 'success'
        self._set()

    def fail(self):
        self.status = 'fail'
        self._set()

    def _set(self):
        # We skip the event if the server is not available
        if not self._server_live:
            return
        r = requests.post(
            f'http://{self.server_address}:{self.server_port}/events'
            f'?producer={self.producer}&name={self.name}&status={self.status}',
        )
        if r.status_code != 200:
            logger.error(f'Failed to set event {self.producer}:{self.name}: {self.status}')
        else:
            logger.debug(f'Set event {self.producer}:{self.name}: {self.status}')
