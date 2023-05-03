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
    def __init__(self, name, producer):
        self.name = name
        self.producer = producer
        self.timestamp = time.time()
        self.server_address = SERVER_ADDRESS
        self.server_port = SERVER_PORT

    def __repr__(self):
        return f'<Event {self.producer}:{self.name}>'

    def set(self):
        r = requests.post(
            f'http://{self.server_address}:{self.server_port}/events?producer={self.producer}&name={self.name}',
        )
        if r.status_code != 200:
            logger.error(f'Failed to set event {self.producer}:{self.name}')
        else:
            logger.debug(f'Set event {self.producer}:{self.name}')
