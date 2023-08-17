#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 02, 2023
"""
import abc
import fnmatch
import itertools
import logging
from collections import defaultdict
from queue import Queue
from threading import Event as ThreadEvent, Thread

from dataci.db.workflow import get_all_latest_workflow_schedule
from dataci.models import Event

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
            workflows = get_all_latest_workflow_schedule()
            self.unsubscribe_all()
            for workflow_dict in workflows:
                identifier = f'{workflow_dict["workspace"]}.{workflow_dict["name"]}@{workflow_dict["version"]}'
                for event in workflow_dict['trigger']:
                    self.subscribe(event, identifier)
            logger.debug(f'Current schedule map: {self._schedule_map}')
            if self._scanner_flag.wait(60):
                break

    def get(self, event_pattern):
        # Parse event pattern
        event = Event.from_str(event_pattern)
        # Get all possible event patterns
        event_type_patterns = {event.producer_type, '*'}
        producer_patterns = {
            event.producer,  # workspace_name.producer_name@version
            event.producer.split('@')[0],  # workspace_name.producer_name
            event.producer.split('@')[0] + '@*',  # workspace_name.producer_name@*
            event.producer.split('@')[0] + '@' + event.producer_alias,  # workspace_name.producer_name@version_tag
            '*',
        }
        event_name_patterns = {event.name, '*'}
        status_patterns = {event.status, '*'}

        event_list = {
            ':'.join(patterns)
            for patterns in itertools.product(
                event_type_patterns, producer_patterns, event_name_patterns, status_patterns
            )
        }

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
