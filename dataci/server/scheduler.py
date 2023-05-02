#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 03, 2023
"""
import logging
from threading import Thread

from dataci.server import EXECUTION_QUEUE, QUEUE_END

logger = logging.getLogger(__name__)


class Scheduler(object):
    def __init__(self):
        self._thread = Thread(target=self.runner)

    def runner(self):
        while True:
            workflow_identifier = EXECUTION_QUEUE.get()
            if workflow_identifier is QUEUE_END:
                break
            # do some task scheduling

    def start(self):
        logger.info('DataCI Scheduler start')
        self._thread.start()

    def join(self):
        self._thread.join()
        logger.info('DataCI Scheduler exit')
