#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 02, 2023
"""
import logging
from collections import defaultdict
from threading import Thread

from dataci.server import EVENT_QUEUE, EXECUTION_QUEUE, QUEUE_END

logger = logging.getLogger(__name__)


class Trigger(object):
    def __init__(self):
        # { event_name: [action1, action2, ...] }
        self._execution_map = defaultdict(list)
        self._thread = Thread(target=self.runner)

    def runner(self):
        while True:
            event = EVENT_QUEUE.get()
            if event is QUEUE_END:
                break
            logger.info(f'Received event: {event}')
            if event not in self._execution_map:
                continue
            for workflow_identifier in self._execution_map[event]:
                EXECUTION_QUEUE.put(workflow_identifier)

    def subscribe(self, event_name, workflow_identifier):
        self._execution_map[event_name].append(workflow_identifier)

    def unsubscribe(self, event_name, workflow_identifier):
        self._execution_map[event_name].remove(workflow_identifier)

    def start(self):
        logger.info('Start DataCI Trigger')
        self._thread.start()

    def join(self):
        self._thread.join()
        logger.info('DataCI Trigger exit')
