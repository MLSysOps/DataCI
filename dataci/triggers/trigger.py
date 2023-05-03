#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 02, 2023
"""
import logging
from collections import defaultdict
from threading import Thread, Event as ThreadEvent

from dataci.db.workflow import get_all_latest_workflow_schedule
from dataci.server import EVENT_QUEUE, EXECUTION_QUEUE, QUEUE_END

logger = logging.getLogger(__name__)


class Trigger(object):
    def __init__(self):
        # { event_name: [action1, action2, ...] }
        self._schedule_map = defaultdict(set)
        self._runner_thread = Thread(target=self.runner)
        self._scanner_thread = Thread(target=self.scan)
        self._scanner_flag = ThreadEvent()

    def runner(self):
        while True:
            event = EVENT_QUEUE.get()
            if event is QUEUE_END:
                break
            logger.debug(f'Received event: {event}')
            if event not in self._schedule_map:
                continue
            for workflow_identifier in self._schedule_map[event]:
                EXECUTION_QUEUE.put(workflow_identifier)
                logger.debug(f'Put workflow {workflow_identifier} into execution queue')

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
