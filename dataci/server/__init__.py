#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 02, 2023
"""
from queue import Queue

EVENT_QUEUE = Queue()
EXECUTION_QUEUE = Queue()
QUEUE_END = object()

SERVER_ADDRESS = 'localhost'
SERVER_PORT = 9000
