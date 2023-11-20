#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 20, 2023
"""
from typing import List, Optional, Union

from openlineage.client.run import DatasetEvent, JobEvent
from fastapi import FastAPI

from metadata.models import RunEvent, DatasetEvent, JobEvent

app = FastAPI()

# Record lineage information as schema defined as OpenLineage (2-0-2)
# https://openlineage.io/apidocs/openapi/
@app.post('/lineage', summary='Send an event related to the state of a run')
def post_lineage(event: Union[RunEvent, DatasetEvent, JobEvent]):
    """Updates a run state for a job.
    """
    print(event)
    return {'status': 'success'}
