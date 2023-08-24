#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 02, 2023
"""
import abc
import fnmatch
import logging
from collections import defaultdict
from queue import Queue
from threading import Event as ThreadEvent, Thread

from dataci.db.workflow import get_all_workflow_schedule

logger = logging.getLogger(__name__)

EVENT_QUEUE = Queue()
QUEUE_END = object()


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
            workflows = get_all_workflow_schedule()
            self.unsubscribe_all()
            for workflow_dict in workflows:
                # We do not care about non-published workflows, since those are not ready to be triggered
                if workflow_dict['version_tag'] is not None:
                    identifier = f'{workflow_dict["workspace"]}.{workflow_dict["name"]}@{workflow_dict["version_tag"]}'
                    for event in workflow_dict['trigger']:
                        self.subscribe(event, identifier)
            logger.debug(f'Current schedule map: {self._schedule_map}')
            if self._scanner_flag.wait(10):
                break

    def get(self, event_str):
        # Substitute event producer name with producer alias
        producer_type, producer, event_name, status, alias = event_str.split(':')
        event_list = {':'.join([producer_type, producer, event_name, status])}
        if alias:
            producer = producer.split('@')[0] + '@' + alias
            event_list.add(':'.join([producer_type, producer, event_name, status]))

        # Get all workflows that subscribe to the event
        # Since the event in scheduler map is a glob pattern, we need to use fnmatch to match the raised event
        workflows = set()
        for event_pattern in self._schedule_map:
            if fnmatch.filter(event_list, event_pattern):
                workflows.update(self._schedule_map[event_pattern])
        return workflows

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
