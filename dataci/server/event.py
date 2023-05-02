#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 02, 2023

Event for trigger.
"""
import logging
import time

from dataci.server import EVENT_QUEUE, SERVER_ADDRESS, SERVER_PORT

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
        EVENT_QUEUE.put(f'{self.producer}:{self.name}')
