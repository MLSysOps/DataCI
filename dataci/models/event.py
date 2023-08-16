#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 15, 2023
"""
import logging

import requests

from dataci.config import SERVER_ADDRESS, SERVER_PORT, DISABLE_EVENT

logger = logging.getLogger(__name__)


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
            f'?producer={self.producer}&name={self.name}&status={self.status}',
        )
        if r.status_code != 200:
            logger.error(f'Failed to set event {self.producer}:{self.name}:{self.status}')
        else:
            logger.debug(f'Set event {self.producer}:{self.name}:{self.status}')
