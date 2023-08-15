#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 15, 2023
"""
import logging
from threading import Event as ThreadEvent

import requests

from dataci.config import SERVER_ADDRESS, SERVER_PORT

logger = logging.getLogger(__name__)

DISABLE_EVENT = ThreadEvent()

# Try if server is available
try:
    r = requests.get(f'http://{SERVER_ADDRESS}:{SERVER_PORT}/live')
    if r.status_code == 200:
        DISABLE_EVENT.clear()
        logger.debug(f'DataCI server is live: {SERVER_ADDRESS}:{SERVER_PORT}')
    logger.debug(f'DataCI server response: {r.status_code}')
except requests.exceptions.ConnectionError:
    logger.warning(
        f'DataCI server is not live: {SERVER_ADDRESS}:{SERVER_PORT}. Event trigger is disabled.'
    )
    DISABLE_EVENT.set()


class Event(object):
    def __init__(self, name, producer, status=None):
        self.name = name
        self.producer = producer
        self.status = status

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
        if DISABLE_EVENT.is_set():
            return
        r = requests.post(
            f'http://{SERVER_ADDRESS}:{SERVER_PORT}/events'
            f'?producer={self.producer}&event={self.name}&status={self.status}',
        )
        if r.status_code != 200:
            logger.error(f'Failed to set event {self.producer}:{self.name}:{self.status}')
        else:
            logger.debug(f'Set event {self.producer}:{self.name}:{self.status}')
