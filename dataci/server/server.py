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

from dataci.config import SERVER_ADDRESS, SERVER_PORT, DISABLE_EVENT
from dataci.plugins.orchestrator.airflow import Trigger
from dataci.server.trigger import EVENT_QUEUE, QUEUE_END

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
trigger = Trigger()


@app.get('/live')
def live():
    return


@app.post('/events')
def set_event(producer: str, name: str, status: str = None):
    status = status or ''
    EVENT_QUEUE.put(f'{producer}:{name}:{status}')
    logger.debug(f'Received event {producer}:{name}:{status}')
    return {'status': 'ok'}


@app.on_event('startup')
def startup_event():
    trigger.start()
    DISABLE_EVENT.clear()


@app.on_event('shutdown')
def shutdown_event():
    EVENT_QUEUE.put_nowait(QUEUE_END)
    trigger.join()


def main():
    uvicorn.run(app, host=SERVER_ADDRESS, port=SERVER_PORT)


if __name__ == '__main__':
    main()
