#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 02, 2023
"""
import abc
import logging
import time
from collections import defaultdict
from queue import Queue
from threading import Event as ThreadEvent, Thread

import requests

from dataci.config import SERVER_ADDRESS, SERVER_PORT
from dataci.db.workflow import get_all_latest_workflow_schedule

logger = logging.getLogger(__name__)

DISABLE_EVENT = ThreadEvent()
EVENT_QUEUE = Queue()
QUEUE_END = object()
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
        self.timestamp = time.time()

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


class Trigger(abc.ABC):
    def __init__(self):
        # { event_name: [action1, action2, ...] }
        self._schedule_map = defaultdict(set)
        self._runner_thread = Thread(target=self.runner)
        self._scanner_thread = Thread(target=self.scan)
        self._scanner_flag = ThreadEvent()
        self.logger = logger

    @abc.abstractmethod
    def runner(self):
        raise NotImplementedError

    def scan(self):
        # Scan for all registered schedule workflows upon events every 60 seconds.
        while True:
            workflows = get_all_latest_workflow_schedule()
            self.unsubscribe_all()
            for workflow_dict in workflows:
                identifier = f'{workflow_dict["workspace"]}.{workflow_dict["name"]}@{workflow_dict["version"]}'
                for event in workflow_dict['schedule']:
                    self.subscribe(event, identifier)
            logger.debug(f'Current schedule map: {self._schedule_map}')
            if self._scanner_flag.wait(60):
                break

    def subscribe(self, event_name, workflow_identifier):
        self._schedule_map[event_name].add(workflow_identifier)

    def unsubscribe(self, event_name, workflow_identifier):
        self._schedule_map[event_name].discard(workflow_identifier)

    def unsubscribe_all(self):
        self._schedule_map.clear()

    def start(self):
        logger.info('Start DataCI Trigger')
        self._scanner_thread.start()
        self._runner_thread.start()

    def join(self):
        self._scanner_flag.set()
        self._scanner_thread.join()
        self._runner_thread.join()
        logger.info('DataCI Trigger exit')
