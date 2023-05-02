#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 03, 2023

Main entry for dataci orchestration server.
"""
import logging

from dataci.server.scheduler import Scheduler
from dataci.server.trigger import Trigger

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    # TODO: we should set up a API server to accept event requests from client.
    logger.info('Starting dataci server...')
    trigger = Trigger()
    scheduler = Scheduler()
    trigger.start()
    scheduler.start()
    trigger.join()
    scheduler.join()
    logger.info('Dataci server stopped.')
