#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 15, 2023
"""
import logging
from urllib.parse import urlparse

import requests

from dataci.config import SERVER_ADDRESS, SERVER_PORT, DISABLE_EVENT

logger = logging.getLogger(__name__)


class Event(object):
    def __init__(self, name, type, producer, status='*'):
        self.name = name
        self.type = type
        self.producer = producer
        self.status = status

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
            f'?type={self.type}&producer={self.producer}&name={self.name}&status={self.status}',
        )
        if r.status_code != 200:
            logger.error(f'Failed to set event {self.producer}:{self.name}:{self.status}')
        else:
            logger.debug(f'Set event {self.producer}:{self.name}:{self.status}')

    @classmethod
    def from_str(cls, event_str):
        type, producer, name, status = event_str.split(':')
        return cls(name, type, producer, status)

    def __repr__(self):
        return f'<Event {self.type}:{self.producer}:{self.name}:{self.status}>'

    def __str__(self):
        return f'{self.type}:{self.producer}:{self.name}:{self.status}'
