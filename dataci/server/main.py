#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 03, 2023

Main entry for dataci orchestration server.
"""
import logging

import uvicorn
from fastapi import FastAPI

from dataci.server import EVENT_QUEUE, QUEUE_END, EXECUTION_QUEUE, SERVER_ADDRESS, SERVER_PORT
from dataci.server.scheduler import Scheduler
from dataci.server.trigger import Trigger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
trigger = Trigger()
scheduler = Scheduler()


@app.post('/events')
def set_event(producer: str, name: str):
    EVENT_QUEUE.put(f'{producer}:{name}')
    logger.debug(f'Received event {producer}:{name}')
    return {'status': 'ok'}


@app.on_event('startup')
def startup_event():
    trigger.start()
    scheduler.start()


@app.on_event('shutdown')
def shutdown_event():
    EVENT_QUEUE.put_nowait(QUEUE_END)
    EXECUTION_QUEUE.put_nowait(QUEUE_END)
    trigger.join()
    scheduler.join()


if __name__ == '__main__':
    uvicorn.run(app, host=SERVER_ADDRESS, port=SERVER_PORT)
