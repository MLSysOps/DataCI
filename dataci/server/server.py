#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 03, 2023

Main entry for dataci orchestration server.
"""
import logging
import os
import subprocess
import sys

import uvicorn
from fastapi import FastAPI

from dataci.config import SERVER_ADDRESS, SERVER_PORT, DISABLE_EVENT
from dataci.plugins.orchestrator.airflow import Trigger
from dataci.server.trigger import EVENT_QUEUE, QUEUE_END

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_airflow():
    global orchestration_runner
    # Run orchestration server on a separate thread
    export_env = dict(os.environ)
    export_env['PATH'] = f'{os.environ["PATH"]}:{os.path.dirname(sys.executable)}'
    orchestration_runner = subprocess.Popen(
        ['python', '-m', 'airflow', 'standalone'],
        env=export_env,
    )


app = FastAPI()
trigger = Trigger()
orchestration_runner: subprocess.Popen = None


@app.get('/live')
def live():
    return


@app.post('/events')
def set_event(type: str, producer: str, name: str, alias: str = '', status: str = '*'):
    EVENT_QUEUE.put(f'{type}:{producer}:{name}:{status}:{alias}')
    logger.debug(f'Received event {type}:{producer}:{name}:{status}:{alias}')
    return {'status': 'ok'}


@app.on_event('startup')
def startup():
    trigger.start()
    DISABLE_EVENT.clear()
    run_airflow()


@app.on_event('shutdown')
def shutdown():
    EVENT_QUEUE.put_nowait(QUEUE_END)
    trigger.join()
    if orchestration_runner is not None:
        orchestration_runner.terminate()
        orchestration_runner.wait()


def main():
    uvicorn.run(app, host=SERVER_ADDRESS, port=SERVER_PORT)


if __name__ == '__main__':
    main()
