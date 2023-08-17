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
    def __init__(self, name, producer, producer_type, status='*', producer_alias=None):
        self.name = name
        self.producer_type = producer_type
        self.producer = producer
        self.status = status

        # We specify the producer alias for the event, so its version tag is also raised with the event triggered
        self.producer_alias = producer_alias

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
            f'?type={self.producer_type}&producer={self.producer}&alias={self.producer_alias or ""}'
            f'&name={self.name}&status={self.status}',
        )
        if r.status_code != 200:
            logger.error(f'Failed to set event {self.producer}:{self.name}:{self.status}')
        else:
            logger.debug(f'Set event {self.producer}:{self.name}:{self.status}')

    @classmethod
    def from_str(cls, event_str):
        event_splits = event_str.split(':')
        if len(event_splits) <= 3:
            raise ValueError(f'Invalid event string {event_str}')
        type, producer, name = event_splits[:3]
        status = event_splits[3] if len(event_splits) > 3 else '*'
        alias = event_splits[4] if len(event_splits) > 4 else None
        return cls(name, producer, type, status, alias)

    def __repr__(self):
        return f'<Event {self.producer_type}:{self.producer}:{self.name}:{self.status}>'

    def __str__(self):
        return f'{self.producer_type}:{self.producer}:{self.name}:{self.status}'
